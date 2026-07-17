"""积分三段式服务 — reserve / settle / release / refund / grant。

全部操作在 DB 事务内、account 行级锁 SELECT ... FOR UPDATE 保证并发安全；
每步写 CreditLog 含 balance_after/frozen_after。
reservation_id 串联 reserve/settle/release；(source_type, source_id) 全生命周期
唯一。同 source 同载荷返回原结果，不同载荷返回 409；业务重试必须创建新的
attempt/source_id。
reserve/settle/release/refund/grant 默认各自 commit；调用方需将 reserve 与
任务状态写入合并为单事务时传 commit=False，由调用方统一提交（避免
「积分已冻结但任务拿不到 reservation_id」的孤立冻结，见 08 §4.2）。
"""

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ConflictError, InsufficientCreditsError
from app.core.ids import new_id
from app.models.credit import CreditAccount
from app.models.credit_log import CreditLog
from app.models.credit_reservation import CreditReservation


async def _commit_if_requested(db: AsyncSession, commit: bool) -> None:
    """服务自管事务时，早退路径也要结束事务并释放锁。"""
    if commit:
        await db.commit()


async def _rollback_if_requested(db: AsyncSession, commit: bool) -> None:
    """服务自管事务失败时必须结束事务并释放锁。"""
    if commit:
        await db.rollback()


async def _lock_account(
    db: AsyncSession, account_id: str, *, require_active: bool = True
) -> CreditAccount:
    """行级锁账户（FOR UPDATE），并校验存在与启用状态。"""
    result = await db.execute(
        select(CreditAccount)
        .where(CreditAccount.id == account_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    acc = result.scalar_one_or_none()
    if acc is None:
        raise ConflictError("积分账户不存在")
    if require_active and acc.status != "active":
        raise ConflictError("积分账户已禁用")
    return acc


async def _lock_reservation(
    db: AsyncSession, reservation_id: str
) -> CreditReservation:
    """行级锁预约记录。"""
    result = await db.execute(
        select(CreditReservation)
        .where(CreditReservation.id == reservation_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    r = result.scalar_one_or_none()
    if r is None:
        raise ConflictError("预扣预约不存在")
    return r


async def _write_log(
    db: AsyncSession,
    *,
    account_id: str,
    log_type: str,
    points_delta: int,
    frozen_delta: int,
    balance_after: int,
    frozen_after: int,
    actor_user_id: str | None = None,
    actor_username: str | None = None,
    target_user_id: str | None = None,
    target_username: str | None = None,
    target_owner_type: str | None = None,
    target_owner_id: str | None = None,
    reservation_id: str | None = None,
    source_type: str | None = None,
    source_id: str | None = None,
    idempotency_key: str | None = None,
    note: str | None = None,
) -> None:
    """写一条流水，记录操作后余额/冻结快照。"""
    db.add(
        CreditLog(
            id=new_id(),
            account_id=account_id,
            actor_user_id=actor_user_id,
            actor_username=actor_username,
            target_user_id=target_user_id,
            target_username=target_username,
            target_owner_type=target_owner_type,
            target_owner_id=target_owner_id,
            reservation_id=reservation_id,
            source_type=source_type,
            source_id=source_id,
            idempotency_key=idempotency_key,
            log_type=log_type,
            points_delta=points_delta,
            frozen_delta=frozen_delta,
            balance_after=balance_after,
            frozen_after=frozen_after,
            note=note,
        )
    )


async def _get_reservation_by_source(
    db: AsyncSession, source_type: str, source_id: str
) -> CreditReservation | None:
    result = await db.execute(
        select(CreditReservation).where(
            CreditReservation.source_type == source_type,
            CreditReservation.source_id == source_id,
        )
    )
    return result.scalar_one_or_none()


def _same_reservation_payload(
    reservation: CreditReservation, account_id: str, estimated: int
) -> bool:
    return (
        reservation.account_id == account_id
        and reservation.reserved_points == estimated
    )


async def reserve(
    db: AsyncSession,
    account_id: str,
    source_type: str,
    source_id: str,
    estimated: int,
    *,
    actor_user_id: str | None = None,
    actor_username: str | None = None,
    commit: bool = True,
) -> str:
    """冻结预扣：frozen += estimated，可用 -= estimated，余额不变。

    返回 reservation_id。幂等：同一 (source_type, source_id) 同载荷返回原预约，
    不同载荷返回 409；预约进入终态后 source 仍不可复用。

    Args:
        commit: 是否在内部提交事务。默认 True（内部 commit）。
            调用方需将 reserve 与任务记录/reservation_id 回写合并为单事务时
            传 False，由调用方统一 commit，避免「积分已冻结但任务拿不到
            reservation_id」的孤立冻结（08 §4.2）。
    """
    if estimated <= 0:
        await _rollback_if_requested(db, commit)
        raise ConflictError("预扣积分必须为正")

    try:
        # source 对应一次不可复用的业务 attempt。相同载荷返回原预约，任何字段
        # 不同均为幂等键冲突；终态也不得用同一 source 再次扣费。
        existing_reservation = await _get_reservation_by_source(
            db, source_type, source_id
        )
        if existing_reservation is not None:
            if _same_reservation_payload(existing_reservation, account_id, estimated):
                await _commit_if_requested(db, commit)
                return existing_reservation.id
            raise ConflictError("该来源已使用且请求载荷不一致")

        acc = await _lock_account(db, account_id)
        # 后到请求可能在账户锁处等待 winner 提交；取得锁后必须重新读取 source。
        existing_reservation = await _get_reservation_by_source(
            db, source_type, source_id
        )
        if existing_reservation is not None:
            if _same_reservation_payload(existing_reservation, account_id, estimated):
                await _commit_if_requested(db, commit)
                return existing_reservation.id
            raise ConflictError("该来源已使用且请求载荷不一致")
        # 可用积分 = balance - frozen
        if acc.balance_points - acc.frozen_points < estimated:
            raise InsufficientCreditsError()

        async with db.begin_nested():
            acc.frozen_points += estimated
            reservation_id = new_id()
            db.add(
                CreditReservation(
                    id=reservation_id,
                    account_id=account_id,
                    source_type=source_type,
                    source_id=source_id,
                    reserved_points=estimated,
                    status="reserved",
                )
            )
            await _write_log(
                db,
                account_id=account_id,
                log_type="reserve",
                points_delta=0,
                frozen_delta=estimated,
                balance_after=acc.balance_points,
                frozen_after=acc.frozen_points,
                actor_user_id=actor_user_id,
                actor_username=actor_username,
                reservation_id=reservation_id,
                source_type=source_type,
                source_id=source_id,
            )
            # commit=False 时也在服务边界内触发 source 唯一约束，避免并发冲突
            # 延迟到调用方提交后变成未转换的 IntegrityError。
            await db.flush()
        if commit:
            await db.commit()
    except IntegrityError as exc:
        existing_reservation = await _get_reservation_by_source(
            db, source_type, source_id
        )
        if (
            existing_reservation is not None
            and _same_reservation_payload(existing_reservation, account_id, estimated)
        ):
            await _commit_if_requested(db, commit)
            return existing_reservation.id
        await _rollback_if_requested(db, commit)
        raise ConflictError("该来源已存在预扣预约") from exc
    except Exception:
        await _rollback_if_requested(db, commit)
        raise
    return reservation_id


async def settle(
    db: AsyncSession,
    reservation_id: str,
    actual: int,
    *,
    actor_user_id: str | None = None,
    actor_username: str | None = None,
    commit: bool = True,
) -> None:
    """结算：balance -= actual，frozen -= reserved，reservation.status=captured。

    幂等：captured 且 actual 相同直接成功，不同 actual 或非法终态返回 409。
    doc 08 §2.4 实测：视频生成 reserved frozen=210、settle actual=784，
    实际结算可超过预扣额度（超出部分从余额补扣），故仅禁止负结算。

    Args:
        commit: 是否在内部提交事务。默认 True。worker 需将积分结算与 job 状态
            更新合并为单事务时传 False，由调用方统一 commit，消除「积分已结算
            但任务仍 running」的不一致窗口（08 §4.2）。
    """
    if actual < 0:
        await _rollback_if_requested(db, commit)
        raise ConflictError("实际消耗不可为负")

    try:
        r = await _lock_reservation(db, reservation_id)
    except Exception:
        await _rollback_if_requested(db, commit)
        raise
    if r.status == "captured":
        if r.actual_points == actual:
            await _commit_if_requested(db, commit)
            return
        if commit:
            await db.rollback()
        raise ConflictError("该预约已按不同实际消耗结算")
    if r.status != "reserved":
        if commit:
            await db.rollback()
        raise ConflictError("该预扣预约已结算或已释放，不可重复结算")

    try:
        acc = await _lock_account(db, r.account_id, require_active=False)
    except Exception:
        await _rollback_if_requested(db, commit)
        raise
    # 释放冻结额度，按实际消耗扣余额（actual 可能 > reserved，差额从余额补扣）。
    # 校验保证结算后可用积分非负：扣减本预约冻结后剩余可用仍须覆盖 actual。
    # 可用 = balance - frozen；结算后 frozen 减去本预约 reserved，故等价于
    # (balance - frozen + reserved) >= actual。仅校验 balance >= actual 不足：
    # 当存在其他活跃冻结预约且 actual > reserved 时，结算后
    # balance_after < frozen_after，可用变负，违反不变量。
    if acc.balance_points - acc.frozen_points + r.reserved_points < actual:
        if commit:
            await db.rollback()
        raise InsufficientCreditsError("可用积分不足以结算实际消耗")
    try:
        acc.frozen_points -= r.reserved_points
        acc.balance_points -= actual
        r.actual_points = actual
        r.status = "captured"
        await _write_log(
            db,
            account_id=acc.id,
            log_type="settle",
            points_delta=-actual,
            frozen_delta=-r.reserved_points,
            balance_after=acc.balance_points,
            frozen_after=acc.frozen_points,
            actor_user_id=actor_user_id,
            actor_username=actor_username,
            reservation_id=reservation_id,
            source_type=r.source_type,
            source_id=r.source_id,
        )
        if commit:
            await db.commit()
    except Exception:
        if commit:
            await db.rollback()
        raise


async def release(
    db: AsyncSession,
    reservation_id: str,
    reason: str | None = None,
    *,
    actor_user_id: str | None = None,
    actor_username: str | None = None,
    commit: bool = True,
) -> None:
    """释放冻结（失败/取消）：frozen -= reserved，reservation.status=released。

    幂等：released 终态直接成功，不依赖失败原因等调用元数据。

    Args:
        commit: 是否在内部提交事务。默认 True。worker 需将释放与 job 状态更新
            合并为单事务时传 False，由调用方统一 commit。
    """
    try:
        r = await _lock_reservation(db, reservation_id)
    except Exception:
        await _rollback_if_requested(db, commit)
        raise
    if r.status == "released":
        await _commit_if_requested(db, commit)
        return
    if r.status != "reserved":
        if commit:
            await db.rollback()
        raise ConflictError("该预扣预约已结算或已释放，不可重复释放")

    try:
        acc = await _lock_account(db, r.account_id, require_active=False)
    except Exception:
        await _rollback_if_requested(db, commit)
        raise
    try:
        acc.frozen_points -= r.reserved_points
        r.status = "released"
        r.released_at = datetime.now(UTC)
        await _write_log(
            db,
            account_id=acc.id,
            log_type="release",
            points_delta=0,
            frozen_delta=-r.reserved_points,
            balance_after=acc.balance_points,
            frozen_after=acc.frozen_points,
            actor_user_id=actor_user_id,
            actor_username=actor_username,
            reservation_id=reservation_id,
            source_type=r.source_type,
            source_id=r.source_id,
            note=reason,
        )
        if commit:
            await db.commit()
    except Exception:
        if commit:
            await db.rollback()
        raise


async def refund(
    db: AsyncSession,
    reservation_id: str,
    points: int | None = None,
    *,
    actor_user_id: str | None = None,
    actor_username: str | None = None,
    note: str | None = None,
    commit: bool = True,
) -> None:
    """完整返还 captured.actual_points，并转入 refunded 终态。

    ``points`` 仅为旧调用点兼容参数，服务不信任也不使用外部退款额。

    Args:
        commit: 是否在内部提交事务。默认 True。worker 需将返还与 job 状态更新
            合并为单事务时传 False，由调用方统一 commit。
    """
    try:
        r = await _lock_reservation(db, reservation_id)
    except Exception:
        await _rollback_if_requested(db, commit)
        raise
    if r.status == "refunded":
        await _commit_if_requested(db, commit)
        return
    # 状态守卫（08 §2.4 状态机）：refunded 终态仅可从 captured 转入。
    # 对 reserved(尚未结算) 或 released(已释放) 的预约执行 refund 会令余额
    # 凭空增加而冻结/状态不变，违反状态机转移约束，故拒绝。
    if r.status != "captured":
        if commit:
            await db.rollback()
        raise ConflictError("仅可对已结算(captured)预约执行返还")
    # 返还额上限校验：不得超过该预约实际结算扣减额，防止凭空增加余额。
    # settle 时写入 actual_points；历史记录缺值时 fail closed，防止以预扣额
    # 代替较低实际扣费而凭空增加余额。
    if r.actual_points is None:
        if commit:
            await db.rollback()
        raise ConflictError("预约缺少实际结算金额，无法自动返还")
    refund_points = r.actual_points
    try:
        acc = await _lock_account(db, r.account_id, require_active=False)
    except Exception:
        await _rollback_if_requested(db, commit)
        raise
    try:
        acc.balance_points += refund_points
        r.status = "refunded"
        await _write_log(
            db,
            account_id=acc.id,
            log_type="refund",
            points_delta=refund_points,
            frozen_delta=0,
            balance_after=acc.balance_points,
            frozen_after=acc.frozen_points,
            actor_user_id=actor_user_id,
            actor_username=actor_username,
            reservation_id=reservation_id,
            source_type=r.source_type,
            source_id=r.source_id,
            note=note,
        )
        if commit:
            await db.commit()
    except Exception:
        if commit:
            await db.rollback()
        raise


async def grant(
    db: AsyncSession,
    from_id: str,
    to_id: str,
    points: int,
    actor_id: str,
    *,
    actor_username: str | None = None,
    target_user_id: str | None = None,
    target_username: str | None = None,
    note: str | None = None,
    idempotency_key: str | None = None,
    commit: bool = True,
) -> None:
    """部门/组织下发：from 账户扣余额，to 账户加余额。

    actor ≠ target 实现代付语义。两个账户按稳定 ID 顺序加行级锁以避免死锁。
    idempotency_key 必填；同载荷重放直接成功，不同载荷返回 409。
    """
    if not idempotency_key or idempotency_key != idempotency_key.strip():
        await _rollback_if_requested(db, commit)
        raise ConflictError("积分下发必须提供非空幂等键")
    if points <= 0:
        await _rollback_if_requested(db, commit)
        raise ConflictError("下发积分必须为正")
    if from_id == to_id:
        await _rollback_if_requested(db, commit)
        raise ConflictError("不可向同一账户下发")

    try:
        prior_logs = await _get_grant_logs(db, idempotency_key)
        if prior_logs:
            replay_target_user_id = (
                target_user_id
                if any(item.target_user_id is not None for item in prior_logs)
                else None
            )
            if _same_grant_payload(
                prior_logs,
                from_id=from_id,
                to_id=to_id,
                points=points,
                actor_id=actor_id,
                target_user_id=replay_target_user_id,
                note=note,
            ):
                await _commit_if_requested(db, commit)
                return
            raise ConflictError("幂等键已用于不同的积分下发请求")

        # 按主键字典序加锁，避免两个并发 grant 反向加锁导致死锁。
        # 两次 _lock_account 均走 SELECT ... FOR UPDATE，第二次命中同一行时
        # SQLAlchemy identity map 返回已加载对象，仍会发出锁语句维持锁语义。
        lock_order = sorted([from_id, to_id])
        locked: dict[str, CreditAccount] = {}
        for aid in lock_order:
            locked[aid] = await _lock_account(db, aid)

        to_acc = locked[to_id]
        target_owner_type = to_acc.owner_type
        target_owner_id = to_acc.owner_id
        if target_owner_type == "user":
            if target_user_id != target_owner_id or not target_username:
                raise ConflictError("用户目标身份不完整或与目标账户不一致")
            resolved_target_user_id = target_owner_id
            resolved_target_username = target_username
        else:
            resolved_target_user_id = None
            resolved_target_username = None

        from_acc = locked[from_id]
        if from_acc.balance_points - from_acc.frozen_points < points:
            raise InsufficientCreditsError()
        async with db.begin_nested():
            source_id = idempotency_key
            from_acc.balance_points -= points
            await _write_log(
                db,
                account_id=from_id,
                log_type="grant",
                points_delta=-points,
                frozen_delta=0,
                balance_after=from_acc.balance_points,
                frozen_after=from_acc.frozen_points,
                actor_user_id=actor_id,
                actor_username=actor_username,
                target_user_id=resolved_target_user_id,
                target_username=resolved_target_username,
                target_owner_type=target_owner_type,
                target_owner_id=target_owner_id,
                source_type="admin_grant",
                source_id=source_id,
                idempotency_key=idempotency_key,
                note=note,
            )

            to_acc.balance_points += points
            await _write_log(
                db,
                account_id=to_id,
                log_type="grant",
                points_delta=points,
                frozen_delta=0,
                balance_after=to_acc.balance_points,
                frozen_after=to_acc.frozen_points,
                actor_user_id=actor_id,
                actor_username=actor_username,
                target_user_id=resolved_target_user_id,
                target_username=resolved_target_username,
                target_owner_type=target_owner_type,
                target_owner_id=target_owner_id,
                source_type="admin_grant",
                source_id=source_id,
                note=note,
            )
            await db.flush()
        await _commit_if_requested(db, commit)
    except IntegrityError as exc:
        await _rollback_if_requested(db, commit)
        if idempotency_key is not None:
            prior_logs = await _get_grant_logs(db, idempotency_key)
            if _same_grant_payload(
                prior_logs,
                from_id=from_id,
                to_id=to_id,
                points=points,
                actor_id=actor_id,
                target_user_id=resolved_target_user_id,
                note=note,
            ):
                await _commit_if_requested(db, commit)
                return
        raise ConflictError("幂等键已用于积分下发请求") from exc
    except Exception:
        await _rollback_if_requested(db, commit)
        raise


async def _get_grant_logs(
    db: AsyncSession, idempotency_key: str
) -> list[CreditLog]:
    result = await db.execute(
        select(CreditLog).where(
            CreditLog.source_type == "admin_grant",
            CreditLog.source_id == idempotency_key,
            CreditLog.log_type == "grant",
        )
    )
    return list(result.scalars().all())


def _same_grant_payload(
    logs: list[CreditLog],
    *,
    from_id: str,
    to_id: str,
    points: int,
    actor_id: str,
    target_user_id: str | None,
    note: str | None,
) -> bool:
    expected = {(from_id, -points), (to_id, points)}
    actual = {(item.account_id, item.points_delta) for item in logs}
    return actual == expected and all(
        item.actor_user_id == actor_id
        and item.target_user_id == target_user_id
        and item.note == note
        for item in logs
    )
