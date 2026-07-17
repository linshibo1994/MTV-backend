"""积分状态机、并发、HTTP 契约、租户边界与 ORM 不变量测试。"""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import (
    CheckConstraint,
    UniqueConstraint,
    create_engine,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.exc import IntegrityError, InvalidRequestError
from sqlalchemy.orm import sessionmaker

from app.api.v1 import credits as credits_api
from app.core.auth import get_current_user
from app.core.config import Settings, settings
from app.core.database import get_db
from app.core.errors import ConflictError, ForbiddenError, InsufficientCreditsError
from app.crud.credit import (
    account_in_management_scope,
    assert_account_accessible,
    get_accounts_for_user,
    get_logs,
    is_account_accessible,
)
from app.models.audit_event import AuditEvent
from app.models.credit import CreditAccount
from app.models.credit_log import CreditLog
from app.models.credit_reservation import CreditReservation
from app.models.user import User
from app.services import credits


class _Scalars:
    def __init__(self, values):
        self._values = values

    def all(self):
        return self._values


class _Result:
    def __init__(self, scalar=None, scalars=None):
        self._scalar = scalar
        self._scalars = scalars or []

    def scalar_one_or_none(self):
        return self._scalar

    def scalars(self):
        return _Scalars(self._scalars)


class _NestedTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _db(*results, rollback=None):
    db = MagicMock()
    db.execute = AsyncMock(side_effect=results)
    db.add = MagicMock()
    db.flush = AsyncMock()
    db.commit = AsyncMock()
    db.rollback = AsyncMock(side_effect=rollback)
    db.begin_nested = MagicMock(return_value=_NestedTransaction())
    return db


def _account(
    account_id="acc",
    *,
    balance=1000,
    frozen=0,
    status="active",
    owner_type="user",
    owner_id="u1",
    org_id="orgA",
    department_id="deptA",
):
    account = CreditAccount(
        id=account_id,
        owner_type=owner_type,
        owner_id=owner_id,
        org_id=org_id,
        department_id=department_id,
        balance_points=balance,
        frozen_points=frozen,
        status=status,
    )
    account.created_at = datetime.now(UTC)
    account.updated_at = datetime.now(UTC)
    return account


def _reservation(
    reservation_id="r1",
    *,
    account_id="acc",
    source_type="video",
    source_id="job1",
    reserved=200,
    actual=None,
    status="reserved",
):
    return CreditReservation(
        id=reservation_id,
        account_id=account_id,
        source_type=source_type,
        source_id=source_id,
        reserved_points=reserved,
        actual_points=actual,
        status=status,
    )


def _user(
    *,
    user_id="u1",
    org_id="orgA",
    department_id="deptA",
    active_org_id="orgA",
    is_admin=False,
    can_manage=True,
    scope="department",
):
    user = User(
        user_id=user_id,
        username=user_id,
        password_hash="x",
        enabled=True,
        is_admin=is_admin,
        account_type="team",
        org_id=org_id,
        department_id=department_id,
        can_manage_accounts=can_manage,
        management_scope=scope,
    )
    setattr(user, "active_org_id", active_org_id)
    return user


async def test_account_lock_uses_for_update_and_populate_existing():
    account = _account()
    db = _db(_Result(account))

    assert await credits._lock_account(db, account.id) is account

    statement = db.execute.await_args.args[0]
    assert statement._for_update_arg is not None
    assert statement.get_execution_options().get("populate_existing") is True


async def test_reservation_lock_uses_for_update_and_populate_existing():
    reservation = _reservation()
    db = _db(_Result(reservation))

    assert await credits._lock_reservation(db, reservation.id) is reservation

    statement = db.execute.await_args.args[0]
    assert statement._for_update_arg is not None
    assert statement.get_execution_options().get("populate_existing") is True


async def test_reserve_same_source_same_payload_is_idempotent():
    existing = _reservation(reserved=210, status="captured")
    db = _db(_Result(existing))

    reservation_id = await credits.reserve(
        db, "acc", "video", "job1", 210
    )

    assert reservation_id == "r1"
    db.add.assert_not_called()
    db.commit.assert_awaited_once()


async def test_reserve_same_source_different_payload_conflicts():
    existing = _reservation(account_id="other", reserved=210, status="reserved")
    db = _db(_Result(existing))

    with pytest.raises(ConflictError):
        await credits.reserve(db, "acc", "video", "job1", 210)


async def test_reserve_commit_false_flushes_constraints_without_commit():
    account = _account(balance=1000)
    db = _db(_Result(None), _Result(account), _Result(None))

    await credits.reserve(
        db, "acc", "video", "job-new", 100, commit=False
    )

    db.flush.assert_awaited_once()
    db.commit.assert_not_awaited()


async def test_reserve_rechecks_same_payload_after_waiting_for_account_lock():
    """并发后到请求取得账户锁后必须重查并返回 winner ID。"""
    account = _account(balance=1000)
    winner = _reservation(reservation_id="winner", reserved=100)
    db = _db(_Result(None), _Result(account), _Result(winner))

    reservation_id = await credits.reserve(
        db, "acc", "video", "job-race", 100
    )

    assert reservation_id == "winner"
    db.add.assert_not_called()
    db.commit.assert_awaited_once()


async def test_reserve_unique_conflict_recovers_same_payload_winner():
    """唯一键竞态 loser 应回收 winner，而不是泄漏 IntegrityError 或重复冻结。"""
    account = _account(balance=1000)
    winner = _reservation(reservation_id="winner", reserved=100)
    db = _db(_Result(None), _Result(account), _Result(None), _Result(winner))
    db.flush = AsyncMock(side_effect=IntegrityError("insert", {}, Exception()))

    reservation_id = await credits.reserve(
        db, "acc", "video", "job-race", 100
    )

    assert reservation_id == "winner"
    db.commit.assert_awaited_once()


@pytest.mark.parametrize(
    "locked_account",
    [
        None,
        _account(status="disabled"),
    ],
)
async def test_reserve_commit_true_rolls_back_missing_or_disabled_account(locked_account):
    db = _db(_Result(None), _Result(locked_account))

    with pytest.raises(ConflictError):
        await credits.reserve(db, "acc", "video", "job-rejected", 100)

    db.rollback.assert_awaited_once()


async def test_reserve_commit_false_rejection_does_not_end_caller_transaction():
    db = _db(_Result(None), _Result(None))

    with pytest.raises(ConflictError):
        await credits.reserve(
            db, "acc", "video", "job-rejected", 100, commit=False
        )

    db.rollback.assert_not_awaited()


async def test_reserve_rechecks_different_payload_after_waiting_for_account_lock():
    account = _account(balance=1000)
    winner = _reservation(
        reservation_id="winner", account_id="acc", source_id="job-race", reserved=99
    )
    db = _db(_Result(None), _Result(account), _Result(winner))

    with pytest.raises(ConflictError, match="载荷"):
        await credits.reserve(db, "acc", "video", "job-race", 100)


async def test_settle_same_actual_is_idempotent():
    reservation = _reservation(actual=120, status="captured")
    db = _db(_Result(reservation))

    await credits.settle(db, "r1", 120)

    db.commit.assert_awaited_once()
    db.add.assert_not_called()


async def test_settle_different_actual_conflicts():
    reservation = _reservation(actual=120, status="captured")
    with pytest.raises(ConflictError):
        await credits.settle(_db(_Result(reservation)), "r1", 121)


async def test_release_replay_is_idempotent():
    reservation = _reservation(status="released")
    release_log = CreditLog(
        id="log1",
        account_id="acc",
        reservation_id="r1",
        log_type="release",
        points_delta=0,
        frozen_delta=-200,
        balance_after=1000,
        frozen_after=0,
        note="cancelled",
    )
    db = _db(_Result(reservation), _Result(release_log))

    await credits.release(db, "r1", reason="cancelled")

    db.commit.assert_awaited_once()
    db.add.assert_not_called()


async def test_release_replay_with_different_reason_conflicts():
    reservation = _reservation(status="released")
    release_log = CreditLog(
        id="log1",
        account_id="acc",
        reservation_id="r1",
        log_type="release",
        points_delta=0,
        frozen_delta=-200,
        balance_after=1000,
        frozen_after=0,
        note="cancelled",
    )

    db = _db(_Result(reservation), _Result(release_log))

    await credits.release(db, "r1", reason="timeout")

    db.commit.assert_awaited_once()
    db.add.assert_not_called()


async def test_refund_replay_returns_original_terminal_result():
    reservation = _reservation(actual=80, status="refunded")
    refund_log = CreditLog(
        id="log1",
        account_id="acc",
        reservation_id="r1",
        log_type="refund",
        points_delta=80,
        frozen_delta=0,
        balance_after=1000,
        frozen_after=0,
    )
    db = _db(_Result(reservation), _Result(refund_log))

    await credits.refund(db, "r1", 1, note="changed request metadata")

    db.commit.assert_awaited_once()
    db.add.assert_not_called()


async def test_refund_ignores_external_points_and_returns_full_actual():
    reservation = _reservation(actual=80, status="captured")
    account = _account(balance=920)
    db = _db(_Result(reservation), _Result(account))

    await credits.refund(db, "r1", 1)

    assert account.balance_points == 1000
    assert reservation.status == "refunded"
    refund_log = next(
        call.args[0]
        for call in db.add.call_args_list
        if isinstance(call.args[0], CreditLog)
    )
    assert refund_log.points_delta == 80


async def test_refund_zero_actual_transitions_without_changing_balance():
    reservation = _reservation(actual=0, status="captured")
    account = _account(balance=1000)
    db = _db(_Result(reservation), _Result(account))

    await credits.refund(db, "r1")

    assert account.balance_points == 1000
    assert reservation.status == "refunded"
    refund_log = next(
        call.args[0]
        for call in db.add.call_args_list
        if isinstance(call.args[0], CreditLog)
    )
    assert refund_log.points_delta == 0


@pytest.mark.parametrize("operation", ["settle", "release", "refund"])
async def test_disabled_account_allows_historical_cleanup(operation):
    if operation == "refund":
        reservation = _reservation(actual=80, status="captured")
        account = _account(balance=920, status="disabled")
        db = _db(_Result(reservation), _Result(account))
        await credits.refund(db, "r1", 1)
        assert account.balance_points == 1000
    elif operation == "settle":
        reservation = _reservation(reserved=200, status="reserved")
        account = _account(balance=1000, frozen=200, status="disabled")
        db = _db(_Result(reservation), _Result(account))
        await credits.settle(db, "r1", 150)
        assert (account.balance_points, account.frozen_points) == (850, 0)
    else:
        reservation = _reservation(reserved=200, status="reserved")
        account = _account(balance=1000, frozen=200, status="disabled")
        db = _db(_Result(reservation), _Result(account))
        await credits.release(db, "r1")
        assert account.frozen_points == 0


@pytest.mark.parametrize("operation", ["settle", "release", "refund"])
async def test_terminal_operations_rollback_locked_reservation_when_account_missing(
    operation,
):
    reservation = _reservation(
        actual=80 if operation == "refund" else None,
        status="captured" if operation == "refund" else "reserved",
    )
    db = _db(_Result(reservation), _Result(None))

    with pytest.raises(ConflictError, match="账户不存在"):
        if operation == "settle":
            await credits.settle(db, reservation.id, 80)
        elif operation == "release":
            await credits.release(db, reservation.id)
        else:
            await credits.refund(db, reservation.id)

    db.rollback.assert_awaited_once()


@pytest.mark.parametrize(
    ("actual", "expected_balance"), [(80, 920), (260, 740)]
)
async def test_settle_actual_may_be_lower_or_higher_than_reserved(
    actual, expected_balance
):
    reservation = _reservation(reserved=200)
    account = _account(balance=1000, frozen=200)
    db = _db(_Result(reservation), _Result(account))

    await credits.settle(db, "r1", actual)

    assert account.balance_points == expected_balance
    assert account.frozen_points == 0
    assert reservation.actual_points == actual
    assert reservation.status == "captured"


async def test_settle_extra_charge_insufficient_is_atomic():
    reservation = _reservation(reserved=200)
    account = _account(balance=300, frozen=250)
    db = _db(_Result(reservation), _Result(account))

    with pytest.raises(InsufficientCreditsError):
        await credits.settle(db, "r1", 260)

    assert (account.balance_points, account.frozen_points) == (300, 250)
    assert reservation.status == "reserved"
    db.add.assert_not_called()


def _create_credit_tables(engine) -> None:
    CreditAccount.__table__.create(engine)
    CreditReservation.__table__.create(engine)
    CreditLog.__table__.create(engine)


class _SyncNestedAsyncAdapter:
    def __init__(self, transaction):
        self._transaction = transaction

    async def __aenter__(self):
        self._transaction.__enter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return self._transaction.__exit__(exc_type, exc, tb)


class _SyncSessionAsyncAdapter:
    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._session.close()
        return False

    async def execute(self, *args, **kwargs):
        return self._session.execute(*args, **kwargs)

    async def scalar(self, *args, **kwargs):
        return self._session.scalar(*args, **kwargs)

    def add(self, *args, **kwargs):
        return self._session.add(*args, **kwargs)

    async def flush(self):
        return self._session.flush()

    async def commit(self):
        return self._session.commit()

    async def rollback(self):
        return self._session.rollback()

    async def get(self, *args, **kwargs):
        return self._session.get(*args, **kwargs)

    def begin_nested(self):
        return _SyncNestedAsyncAdapter(self._session.begin_nested())


def _make_async_credit_session(tmp_path, name="credits.sqlite3"):
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / name}")
    _create_credit_tables(engine)
    sync_maker = sessionmaker(engine, expire_on_commit=False)
    return engine, lambda: _SyncSessionAsyncAdapter(sync_maker())


def test_real_transaction_rolls_back_balance_and_reservation_on_log_failure(
    tmp_path,
):
    """由真实 SQLite 事务证明失败回滚，不手工恢复 ORM 待测对象。"""
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'atomic.sqlite3'}")
    _create_credit_tables(engine)
    with engine.begin() as connection:
        connection.execute(
            insert(CreditAccount).values(
                id="acc",
                owner_type="user",
                owner_id="u1",
                balance_points=1000,
                frozen_points=200,
                status="active",
            )
        )
        connection.execute(
            insert(CreditReservation).values(
                id="r1",
                account_id="acc",
                source_type="video",
                source_id="job1",
                reserved_points=200,
                status="reserved",
            )
        )

    connection = engine.connect()
    transaction = connection.begin()
    try:
        connection.execute(
            update(CreditAccount)
            .where(CreditAccount.id == "acc")
            .values(balance_points=900, frozen_points=0)
        )
        connection.execute(
            update(CreditReservation)
            .where(CreditReservation.id == "r1")
            .values(status="captured", actual_points=100)
        )
        with pytest.raises(IntegrityError):
            connection.execute(
                insert(CreditLog).values(
                    id="invalid-log",
                    account_id="acc",
                    log_type="settle",
                    points_delta=-100,
                    frozen_delta=-200,
                    balance_after=-1,
                    frozen_after=0,
                )
            )
        transaction.rollback()
    finally:
        connection.close()

    with engine.connect() as verification:
        account_row = verification.execute(
            select(CreditAccount.balance_points, CreditAccount.frozen_points)
        ).one()
        reservation_row = verification.execute(
            select(CreditReservation.status, CreditReservation.actual_points)
        ).one()
    assert account_row == (1000, 200)
    assert reservation_row == ("reserved", None)
    engine.dispose()


async def test_reserve_unique_loser_uses_savepoint_and_restores_identity_state(
    tmp_path, monkeypatch
):
    """用真实 AsyncSession 执行 reserve 本身，验证唯一竞争 loser 不重复冻结。"""
    engine, maker = _make_async_credit_session(tmp_path, "reserve-race.sqlite3")
    try:
        async with maker() as seed:
            seed.add(
                CreditAccount(
                    id="acc",
                    owner_type="user",
                    owner_id="u1",
                    balance_points=1000,
                    frozen_points=0,
                    status="active",
                )
            )
            await seed.commit()

        original_get = credits._get_reservation_by_source
        calls = 0
        inserted_winner = False

        async def _race_get(db, source_type: str, source_id: str):
            nonlocal calls, inserted_winner
            calls += 1
            found = await original_get(db, source_type, source_id)
            if found is None and calls == 2 and not inserted_winner:
                async with maker() as winner:
                    winner_account = await winner.get(CreditAccount, "acc")
                    assert winner_account is not None
                    winner_account.frozen_points += 100
                    winner.add(
                        CreditReservation(
                            id="winner",
                            account_id="acc",
                            source_type=source_type,
                            source_id=source_id,
                            reserved_points=100,
                            status="reserved",
                        )
                    )
                    winner.add(
                        CreditLog(
                            id="winner-log",
                            account_id="acc",
                            reservation_id="winner",
                            source_type=source_type,
                            source_id=source_id,
                            log_type="reserve",
                            points_delta=0,
                            frozen_delta=100,
                            balance_after=1000,
                            frozen_after=100,
                        )
                    )
                    await winner.commit()
                inserted_winner = True
                return None
            return found

        monkeypatch.setattr(credits, "_get_reservation_by_source", _race_get)

        async with maker() as loser:
            reservation_id = await credits.reserve(
                loser, "acc", "video", "job-race", 100
            )
            assert reservation_id == "winner"
            loser_account = await loser.get(
                CreditAccount, "acc", populate_existing=True
            )
            assert loser_account is not None
            assert loser_account.frozen_points == 100

        async with maker() as verification:
            final_frozen = await verification.scalar(
                select(CreditAccount.frozen_points).where(CreditAccount.id == "acc")
            )
            reservation_count = await verification.scalar(
                select(func.count()).select_from(CreditReservation).where(
                    CreditReservation.source_type == "video",
                    CreditReservation.source_id == "job-race",
                )
            )
            log_count = await verification.scalar(
                select(func.count()).select_from(CreditLog).where(
                    CreditLog.source_type == "video",
                    CreditLog.source_id == "job-race",
                    CreditLog.log_type == "reserve",
                )
            )
        assert final_frozen == 100
        assert reservation_count == 1
        assert log_count == 1
    finally:
        engine.dispose()


def test_department_admin_can_grant_to_user_in_same_department():
    admin = _user(scope="department")
    source = _account(
        "department-account", owner_type="department", owner_id="deptA"
    )
    target = _account(
        "user-account", owner_type="user", owner_id="u2", department_id="deptA"
    )

    assert account_in_management_scope(source, admin) is True
    assert account_in_management_scope(target, admin, allow_user_target=True) is True
    assert account_in_management_scope(target, admin) is False


def test_org_admin_cannot_debit_user_account_but_can_target_it():
    admin = _user(scope="org")
    target = _account("user-account", owner_type="user", owner_id="u2")

    assert account_in_management_scope(target, admin) is False
    assert account_in_management_scope(target, admin, allow_user_target=True) is True


async def test_platform_admin_does_not_implicitly_cross_tenant_for_credits():
    admin = _user(is_admin=True, scope="none", active_org_id="orgA")
    other = _account("other", org_id="orgB", owner_id="orgB", owner_type="org")

    assert await is_account_accessible(_db(_Result(other)), admin, "other") is False
    assert account_in_management_scope(other, admin) is False


async def test_department_scope_cannot_read_old_department_in_other_active_org():
    admin = _user(org_id="orgA", active_org_id="orgB", scope="department")
    old_department = _account(
        "old-department",
        owner_type="department",
        owner_id="deptA",
        org_id="orgA",
    )

    assert (
        await is_account_accessible(
            _db(_Result(old_department)), admin, "old-department"
        )
        is False
    )


async def test_missing_account_is_404_but_unauthorized_account_is_403():
    user = _user(can_manage=False, scope="none")
    with pytest.raises(Exception) as missing:
        await assert_account_accessible(_db(_Result(None)), user, "missing")
    assert missing.value.status_code == 404

    other = _account("other", org_id="orgB", owner_id="orgB", owner_type="org")
    with pytest.raises(ForbiddenError):
        await assert_account_accessible(
            _db(_Result(other), _Result(other)), user, "other"
        )


async def test_account_queries_use_active_org_not_primary_org():
    user = _user(org_id="orgA", active_org_id="orgB")
    db = _db(_Result(scalars=[]))

    await get_accounts_for_user(db, user)

    sql = str(db.execute.await_args.args[0].compile(compile_kwargs={"literal_binds": True}))
    assert "orgB" in sql
    assert "orgA" not in sql


async def test_dirty_org_account_metadata_fails_closed_for_active_tenant():
    user = _user(org_id="orgA", active_org_id="orgA", scope="org")
    dirty = _account(
        "dirty-org",
        owner_type="org",
        owner_id="orgA",
        org_id="orgB",
        department_id=None,
    )

    assert await is_account_accessible(_db(_Result(dirty)), user, dirty.id) is False


async def test_grant_locks_accounts_in_stable_id_order(monkeypatch):
    accounts = {
        "a": _account("a", balance=0),
        "z": _account("z", balance=100),
    }
    lock = AsyncMock(side_effect=lambda db, account_id, **kwargs: accounts[account_id])
    monkeypatch.setattr(credits, "_lock_account", lock)

    await credits.grant(
        _db(_Result(scalars=[])),
        "z",
        "a",
        10,
        "admin",
        target_user_id="u1",
        target_username="u1",
        idempotency_key="stable-lock-order",
    )

    assert [call.args[1] for call in lock.await_args_list] == ["a", "z"]


async def test_grant_without_idempotency_key_fails_before_account_lock(monkeypatch):
    lock = AsyncMock()
    monkeypatch.setattr(credits, "_lock_account", lock)
    db = _db()

    with pytest.raises(ConflictError, match="幂等键"):
        await credits.grant(
            db,
            "a",
            "z",
            10,
            "admin",
            target_user_id="u2",
            target_username="target",
        )

    lock.assert_not_awaited()
    db.rollback.assert_awaited_once()


@pytest.mark.parametrize(
    ("prior_logs", "from_balance", "target_user_id", "target_username", "match"),
    [
        ([], 0, "u2", "target", InsufficientCreditsError),
        (
            [
                CreditLog(
                    id="old1", account_id="a", actor_user_id="other",
                    target_user_id="u2", log_type="grant", points_delta=-5,
                    frozen_delta=0, balance_after=95, frozen_after=0,
                    source_type="admin_grant", source_id="same-key",
                ),
                CreditLog(
                    id="old2", account_id="z", actor_user_id="other",
                    target_user_id="u2", log_type="grant", points_delta=5,
                    frozen_delta=0, balance_after=5, frozen_after=0,
                    source_type="admin_grant", source_id="same-key",
                ),
            ],
            100,
            "u2",
            "target",
            ConflictError,
        ),
        ([], 100, None, None, ConflictError),
    ],
)
async def test_grant_commit_true_rejected_paths_roll_back_locks(
    monkeypatch, prior_logs, from_balance, target_user_id, target_username, match
):
    accounts = {
        "a": _account("a", balance=from_balance),
        "z": _account("z", balance=0, owner_id="u2"),
    }
    monkeypatch.setattr(
        credits,
        "_lock_account",
        AsyncMock(side_effect=lambda db, account_id, **kwargs: accounts[account_id]),
    )
    db = _db(_Result(scalars=prior_logs))

    with pytest.raises(match):
        await credits.grant(
            db,
            "a",
            "z",
            10,
            "admin",
            target_user_id=target_user_id,
            target_username=target_username,
            idempotency_key="same-key",
        )

    db.rollback.assert_awaited_once()


async def test_grant_commit_false_success_does_not_end_caller_transaction(monkeypatch):
    accounts = {
        "a": _account("a", balance=100),
        "z": _account("z", balance=0, owner_id="u2"),
    }
    monkeypatch.setattr(
        credits,
        "_lock_account",
        AsyncMock(side_effect=lambda db, account_id, **kwargs: accounts[account_id]),
    )
    db = _db(_Result(scalars=[]))

    await credits.grant(
        db,
        "a",
        "z",
        10,
        "admin",
        target_user_id="u2",
        target_username="target",
        idempotency_key="same-key",
        commit=False,
    )

    assert (accounts["a"].balance_points, accounts["z"].balance_points) == (90, 10)
    db.commit.assert_not_awaited()
    db.rollback.assert_not_awaited()


async def test_grant_commit_false_flushes_inside_savepoint(monkeypatch):
    accounts = {
        "a": _account("a", balance=100),
        "z": _account("z", balance=0, owner_id="u2"),
    }
    monkeypatch.setattr(
        credits,
        "_lock_account",
        AsyncMock(side_effect=lambda db, account_id, **kwargs: accounts[account_id]),
    )
    db = _db(_Result(scalars=[]))

    await credits.grant(
        db,
        "a",
        "z",
        10,
        "admin",
        target_user_id="u2",
        target_username="target",
        idempotency_key="savepoint-key",
        commit=False,
    )

    db.begin_nested.assert_called_once()
    db.flush.assert_awaited_once()
    db.commit.assert_not_awaited()
    db.rollback.assert_not_awaited()


async def test_grant_same_idempotency_key_same_payload_is_noop(monkeypatch):
    accounts = {
        "a": _account("a", balance=90),
        "z": _account("z", balance=10, owner_id="u2"),
    }
    lock = AsyncMock(side_effect=lambda db, account_id, **kwargs: accounts[account_id])
    monkeypatch.setattr(credits, "_lock_account", lock)
    debit = CreditLog(
        id="l1", account_id="a", actor_user_id="admin", target_user_id="u2",
        log_type="grant", points_delta=-10, frozen_delta=0,
        balance_after=90, frozen_after=0, source_type="admin_grant",
        source_id="request-1",
    )
    credit = CreditLog(
        id="l2", account_id="z", actor_user_id="admin", target_user_id="u2",
        log_type="grant", points_delta=10, frozen_delta=0,
        balance_after=10, frozen_after=0, source_type="admin_grant",
        source_id="request-1",
    )
    db = _db(_Result(scalars=[debit, credit]))

    await credits.grant(
        db, "a", "z", 10, "admin", target_user_id="u2",
        target_username="target-user",
        idempotency_key="request-1",
    )

    assert (accounts["a"].balance_points, accounts["z"].balance_points) == (90, 10)
    db.add.assert_not_called()
    db.commit.assert_awaited_once()


async def test_grant_replay_succeeds_before_disabled_account_lock(monkeypatch):
    lock = AsyncMock(side_effect=ConflictError("积分账户已禁用"))
    monkeypatch.setattr(credits, "_lock_account", lock)
    prior_logs = [
        CreditLog(
            id="l1", account_id="a", actor_user_id="admin", target_user_id="u2",
            log_type="grant", points_delta=-10, frozen_delta=0,
            balance_after=90, frozen_after=0, source_type="admin_grant",
            source_id="disabled-replay",
        ),
        CreditLog(
            id="l2", account_id="z", actor_user_id="admin", target_user_id="u2",
            log_type="grant", points_delta=10, frozen_delta=0,
            balance_after=10, frozen_after=0, source_type="admin_grant",
            source_id="disabled-replay",
        ),
    ]
    db = _db(_Result(scalars=prior_logs))

    await credits.grant(
        db,
        "a",
        "z",
        10,
        "admin",
        target_user_id="u2",
        target_username="target-user",
        idempotency_key="disabled-replay",
    )

    lock.assert_not_awaited()
    db.add.assert_not_called()
    db.commit.assert_awaited_once()


async def test_grant_replay_different_payload_conflicts_before_account_lock(monkeypatch):
    lock = AsyncMock(side_effect=ConflictError("积分账户已禁用"))
    monkeypatch.setattr(credits, "_lock_account", lock)
    prior_logs = [
        CreditLog(
            id="l1", account_id="a", actor_user_id="admin", target_user_id="u2",
            log_type="grant", points_delta=-10, frozen_delta=0,
            balance_after=90, frozen_after=0, source_type="admin_grant",
            source_id="disabled-conflict",
        ),
        CreditLog(
            id="l2", account_id="z", actor_user_id="admin", target_user_id="u2",
            log_type="grant", points_delta=10, frozen_delta=0,
            balance_after=10, frozen_after=0, source_type="admin_grant",
            source_id="disabled-conflict",
        ),
    ]
    db = _db(_Result(scalars=prior_logs))

    with pytest.raises(ConflictError, match="幂等键已用于不同"):
        await credits.grant(
            db,
            "a",
            "z",
            11,
            "admin",
            target_user_id="u2",
            target_username="target-user",
            idempotency_key="disabled-conflict",
        )

    lock.assert_not_awaited()
    db.add.assert_not_called()
    db.rollback.assert_awaited_once()


def test_grant_idempotency_key_is_globally_unique_in_orm_metadata():
    unique_columns = {
        tuple(column.name for column in constraint.columns)
        for constraint in CreditLog.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert ("idempotency_key",) in unique_columns


def test_concurrent_grant_claims_are_globally_unique_in_real_sqlite(tmp_path):
    """不同账户对并发使用同 key 时，数据库只能接受一个全局 claim。"""
    db_path = tmp_path / "grant-race.sqlite3"
    engine = create_engine(
        f"sqlite+pysqlite:///{db_path}", connect_args={"timeout": 30}
    )
    _create_credit_tables(engine)
    account_ids = ["a-from", "a-to", "z-from", "z-to"]
    with engine.begin() as connection:
        connection.execute(
            insert(CreditAccount),
            [
                {
                    "id": account_id,
                    "owner_type": "user",
                    "owner_id": f"user-{account_id}",
                    "balance_points": 100,
                    "frozen_points": 0,
                    "status": "active",
                }
                for account_id in account_ids
            ],
        )

    barrier = Barrier(2)

    def insert_claim(account_id: str) -> str:
        barrier.wait()
        try:
            with engine.begin() as connection:
                connection.execute(
                    insert(CreditLog).values(
                        id=f"log-{account_id}",
                        account_id=account_id,
                        idempotency_key="global-request",
                        source_type="admin_grant",
                        source_id="global-request",
                        log_type="grant",
                        points_delta=-10,
                        frozen_delta=0,
                        balance_after=90,
                        frozen_after=0,
                    )
                )
            return "created"
        except IntegrityError:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(insert_claim, ["a-from", "z-from"]))

    assert sorted(outcomes) == ["conflict", "created"]
    engine.dispose()


async def test_grant_concurrent_same_payload_reloads_winner(monkeypatch):
    accounts = {
        "a": _account("a", balance=100),
        "z": _account("z", balance=0, owner_id="u2"),
    }
    monkeypatch.setattr(
        credits,
        "_lock_account",
        AsyncMock(side_effect=lambda db, account_id, **kwargs: accounts[account_id]),
    )
    winner_logs = [
        CreditLog(
            id="winner-debit", account_id="a", actor_user_id="admin",
            target_user_id="u2",
            log_type="grant", points_delta=-10, frozen_delta=0,
            balance_after=90, frozen_after=0, source_type="admin_grant",
            source_id="same-key", note="same",
        ),
        CreditLog(
            id="winner-credit", account_id="z", actor_user_id="admin",
            target_user_id="u2",
            log_type="grant", points_delta=10, frozen_delta=0,
            balance_after=10, frozen_after=0, source_type="admin_grant",
            source_id="same-key", note="same",
        ),
    ]
    db = _db(_Result(scalars=[]), _Result(scalars=winner_logs))
    db.commit = AsyncMock(side_effect=[IntegrityError("insert", {}, Exception()), None])

    await credits.grant(
        db,
        "a",
        "z",
        10,
        "admin",
        target_user_id="u2",
        target_username="target",
        note="same",
        idempotency_key="same-key",
    )

    db.rollback.assert_awaited_once()


async def _use_credit_http_dependencies(client, user, db):
    async def current_user_override():
        yield user

    client._app.dependency_overrides[get_current_user] = current_user_override  # type: ignore[attr-defined]
    client._app.dependency_overrides[get_db] = lambda: db  # type: ignore[attr-defined]


async def test_credit_accounts_and_logs_use_real_http_routes(client):
    user = _user(can_manage=False, scope="none")
    account = _account("acc", balance=100)
    log = CreditLog(
        id="log1",
        account_id="acc",
        log_type="grant",
        points_delta=100,
        frozen_delta=0,
        balance_after=100,
        frozen_after=0,
        created_at=datetime.now(UTC),
    )
    accounts_db = _db(_Result(scalars=[account]))
    await _use_credit_http_dependencies(client, user, accounts_db)
    try:
        accounts_response = await client.get("/v3api/credits/accounts")
        assert accounts_response.status_code == 200
        assert accounts_response.json()["items"][0]["id"] == "acc"
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]

    logs_db = _db(_Result(account), _Result(account), _Result(scalars=[log]))
    await _use_credit_http_dependencies(client, user, logs_db)
    try:
        logs_response = await client.get(
            "/v3api/credits/logs",
            params={"account_id": "acc", "log_type": "grant"},
        )
        assert logs_response.status_code == 200
        assert logs_response.json()["items"][0]["id"] == "log1"
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


async def test_credit_accounts_http_uses_active_org(client):
    user = _user(org_id="orgA", active_org_id="orgB", scope="org")
    db = _db(_Result(scalars=[]))
    await _use_credit_http_dependencies(client, user, db)
    try:
        response = await client.get("/v3api/credits/accounts")
        assert response.status_code == 200
        statement = db.execute.await_args.args[0]
        sql = str(statement.compile(compile_kwargs={"literal_binds": True}))
        assert "orgB" in sql
        assert "orgA" not in sql
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


async def test_credit_logs_http_distinguishes_404_and_403(client):
    user = _user(can_manage=False, scope="none")
    missing_db = _db(_Result(None))
    await _use_credit_http_dependencies(client, user, missing_db)
    try:
        missing = await client.get(
            "/v3api/credits/logs", params={"account_id": "missing"}
        )
        assert missing.status_code == 404
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]

    other = _account("other", owner_type="org", owner_id="orgB", org_id="orgB")
    forbidden_db = _db(_Result(other), _Result(other))
    await _use_credit_http_dependencies(client, user, forbidden_db)
    try:
        forbidden = await client.get(
            "/v3api/credits/logs", params={"account_id": "other"}
        )
        assert forbidden.status_code == 403
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


async def test_credit_grant_http_rejects_missing_target_user(client):
    user = _user(scope="department")
    source = _account(
        "a-source", owner_type="department", owner_id="deptA", balance=100
    )
    target = _account("z-target", owner_type="user", owner_id="missing-user")
    db = _db(_Result(source), _Result(target), _Result(None))
    await _use_credit_http_dependencies(client, user, db)
    try:
        response = await client.post(
            "/v3api/credits/grant",
            headers={"Idempotency-Key": "missing-target-user"},
            json={
                "from_account_id": "a-source",
                "to_account_id": "z-target",
                "points": 10,
            },
        )
        assert response.status_code == 404
        assert response.json() == {"detail": "下发目标用户不存在"}
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


async def test_credit_grant_requires_idempotency_key(client):
    user = _user(can_manage=False, scope="none")
    await _use_credit_http_dependencies(client, user, _db())
    try:
        response = await client.post(
            "/v3api/credits/grant",
            json={
                "from_account_id": "source",
                "to_account_id": "target",
                "points": 10,
            },
        )
        assert response.status_code == 422
        assert response.json() == {"detail": "字段校验失败"}
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


async def test_credit_grant_http_revalidates_target_user_current_department(
    client, monkeypatch
):
    user = _user(scope="department")
    source = _account(
        "a-source", owner_type="department", owner_id="deptA", balance=100
    )
    target = _account(
        "z-target",
        owner_type="user",
        owner_id="u2",
        department_id="deptA",  # 旧账户元数据仍显示同部门
    )
    target_user = _user(
        user_id="u2",
        org_id="orgA",
        department_id="deptB",
        active_org_id="orgA",
        can_manage=False,
        scope="none",
    )
    grant = AsyncMock()
    monkeypatch.setattr(credits_api, "grant_service", grant, raising=False)
    monkeypatch.setattr(
        credits_api,
        "audit_log_independently",
        AsyncMock(return_value=None),
        raising=False,
    )
    db = _db(_Result(source), _Result(target), _Result(target_user), _Result("u2"))
    await _use_credit_http_dependencies(client, user, db)
    try:
        response = await client.post(
            "/v3api/credits/grant",
            headers={"Idempotency-Key": "stale-target-department"},
            json={
                "from_account_id": "a-source",
                "to_account_id": "z-target",
                "points": 10,
            },
        )
        assert response.status_code == 403
        grant.assert_not_awaited()
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


async def test_credit_grant_http_propagates_idempotency_and_writes_audit(
    client, monkeypatch
):
    user = _user(scope="department")
    source = _account(
        "a-source", owner_type="department", owner_id="deptA", balance=100
    )
    target = _account(
        "z-target", owner_type="user", owner_id="u2", balance=0
    )
    db = _db(
        _Result(source),
        _Result(target),
        _Result("target-user"),
        _Result(scalars=[]),
        _Result(source),
        _Result(target),
    )
    audit = AsyncMock(return_value=AuditEvent(action="credit_grant"))
    monkeypatch.setattr(
        credits_api, "audit_log_independently", audit, raising=False
    )
    await _use_credit_http_dependencies(client, user, db)
    try:
        response = await client.post(
            "/v3api/credits/grant",
            headers={"Idempotency-Key": "http-request-1"},
            json={
                "from_account_id": "a-source",
                "to_account_id": "z-target",
                "points": 10,
                "note": "approved",
            },
        )
        assert response.status_code == 200
        assert response.json() == {"detail": "下发成功"}
        grant_logs = [
            call.args[0]
            for call in db.add.call_args_list
            if isinstance(call.args[0], CreditLog)
        ]
        assert len(grant_logs) == 2
        assert all(log.source_id == "http-request-1" for log in grant_logs)
        assert all(log.actor_user_id == "u1" for log in grant_logs)
        assert all(log.target_user_id == "u2" for log in grant_logs)
        assert all(log.target_username == "target-user" for log in grant_logs)
        audit.assert_awaited_once()
        audit_payload = audit.await_args.kwargs
        assert audit_payload["action"] == "credit_grant"
        assert "idempotency_key" not in str(audit_payload)
        assert "approved" not in str(audit_payload)
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("balance", "prior_logs", "expected_status"),
    [
        (0, [], 402),
        (
            100,
            [
                CreditLog(
                    id="old1", account_id="a-source", actor_user_id="other",
                    target_user_id="u2", log_type="grant", points_delta=-5,
                    frozen_delta=0, balance_after=95, frozen_after=0,
                    source_type="admin_grant", source_id="http-conflict",
                ),
                CreditLog(
                    id="old2", account_id="z-target", actor_user_id="other",
                    target_user_id="u2", log_type="grant", points_delta=5,
                    frozen_delta=0, balance_after=5, frozen_after=0,
                    source_type="admin_grant", source_id="http-conflict",
                ),
            ],
            409,
        ),
    ],
)
async def test_credit_grant_http_maps_402_and_409(
    client, monkeypatch, balance, prior_logs, expected_status
):
    user = _user(scope="department")
    source = _account(
        "a-source", owner_type="department", owner_id="deptA", balance=balance
    )
    target = _account("z-target", owner_type="user", owner_id="u2", balance=0)
    db = _db(
        _Result(source), _Result(target), _Result("target-user"),
        _Result(scalars=prior_logs), _Result(source), _Result(target),
    )
    audit = AsyncMock(side_effect=RuntimeError("audit unavailable"))
    monkeypatch.setattr(
        credits_api,
        "audit_log_independently",
        audit,
        raising=False,
    )
    await _use_credit_http_dependencies(client, user, db)
    try:
        response = await client.post(
            "/v3api/credits/grant",
            headers={"Idempotency-Key": "http-conflict"},
            json={
                "from_account_id": "a-source",
                "to_account_id": "z-target",
                "points": 10,
            },
        )
        assert response.status_code == expected_status
        audit.assert_awaited_once()
        assert audit.await_args.kwargs["action"] == "credit_grant_failed"
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


async def test_credit_grant_denial_is_audited_and_audit_failure_keeps_403(
    client, monkeypatch
):
    user = _user(can_manage=False, scope="none")
    audit = AsyncMock(side_effect=RuntimeError("audit unavailable"))
    independent_audit = AsyncMock(
        side_effect=AssertionError("拒绝审计不得打开独立数据库会话")
    )
    monkeypatch.setattr(
        credits_api, "audit_log", audit, raising=False
    )
    monkeypatch.setattr(
        credits_api,
        "audit_log_independently",
        independent_audit,
        raising=False,
    )
    await _use_credit_http_dependencies(client, user, _db())
    try:
        response = await client.post(
            "/v3api/credits/grant",
            headers={"Idempotency-Key": "denied-request"},
            json={
                "from_account_id": "source",
                "to_account_id": "target",
                "points": 10,
            },
        )
        assert response.status_code == 403
        audit.assert_awaited_once()
        assert audit.await_args.args[0] is not None
        assert audit.await_args.kwargs["action"] == "credit_access_denied"
        assert "denied-request" not in str(audit.await_args.kwargs)
        independent_audit.assert_not_awaited()
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


async def test_non_user_grant_target_keeps_user_fields_semantically_empty(
    client, monkeypatch
):
    user = _user(scope="org")
    source = _account(
        "a-org", owner_type="org", owner_id="orgA", balance=100
    )
    target = _account(
        "z-dept", owner_type="department", owner_id="deptA", balance=0
    )
    db = _db(
        _Result(source), _Result(target), _Result(scalars=[]),
        _Result(source), _Result(target),
    )
    monkeypatch.setattr(
        credits_api,
        "audit_log_independently",
        AsyncMock(return_value=None),
        raising=False,
    )
    await _use_credit_http_dependencies(client, user, db)
    try:
        response = await client.post(
            "/v3api/credits/grant",
            headers={"Idempotency-Key": "owner-target"},
            json={
                "from_account_id": "a-org",
                "to_account_id": "z-dept",
                "points": 10,
            },
        )
        assert response.status_code == 200
        logs = [
            call.args[0]
            for call in db.add.call_args_list
            if isinstance(call.args[0], CreditLog)
        ]
        assert logs
        assert all(log.target_user_id is None for log in logs)
        assert all(log.target_username is None for log in logs)
        assert all(log.target_owner_type == "department" for log in logs)
        assert all(log.target_owner_id == "deptA" for log in logs)
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


def test_test_settings_do_not_attempt_to_read_dotenv(monkeypatch):
    def fail_if_dotenv_path_is_checked(self: Path) -> bool:
        raise AssertionError("测试不得读取 .env 文件")

    monkeypatch.setattr(Path, "is_file", fail_if_dotenv_path_is_checked)
    configured = Settings()
    assert configured.app_env == "test"


def test_test_environment_uses_non_network_service_sentinels():
    assert settings.app_env == "test"
    assert "localhost" not in settings.database_url
    assert "127.0.0.1" not in settings.database_url
    assert settings.redis_url.startswith("redis+unix://")


async def test_http_db_dependency_fails_closed_without_override(client):
    user = _user(can_manage=False, scope="none")

    async def current_user_override():
        yield user

    client._app.dependency_overrides[get_current_user] = current_user_override  # type: ignore[attr-defined]
    try:
        with pytest.raises(AssertionError, match="禁止访问默认会话"):
            await client.get("/v3api/credits/accounts")
    finally:
        client._app.dependency_overrides.pop(get_current_user, None)  # type: ignore[attr-defined]


async def test_logs_have_stable_descending_order():
    db = _db(_Result(scalars=[]))
    await get_logs(db, "acc")
    sql = str(db.execute.await_args.args[0])
    assert "credit_logs.created_at DESC, credit_logs.id DESC" in sql


async def test_accounts_have_stable_ascending_order():
    db = _db(_Result(scalars=[]))
    await get_accounts_for_user(db, _user())
    sql = str(db.execute.await_args.args[0])
    assert "credit_accounts.created_at, credit_accounts.id" in sql


@pytest.mark.parametrize("operation", ["update", "delete"])
def test_credit_logs_are_immutable_through_ordinary_orm_paths(tmp_path, operation):
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'immutable.sqlite3'}")
    _create_credit_tables(engine)
    maker = sessionmaker(engine, expire_on_commit=False)
    with maker() as session:
        session.add(
            CreditAccount(
                id="acc",
                owner_type="user",
                owner_id="u1",
                balance_points=100,
                frozen_points=0,
                status="active",
            )
        )
        log = CreditLog(
            id="log",
            account_id="acc",
            log_type="grant",
            points_delta=100,
            frozen_delta=0,
            balance_after=100,
            frozen_after=0,
        )
        session.add(log)
        session.commit()

        if operation == "update":
            log.note = "tampered"
        else:
            session.delete(log)

        with pytest.raises(InvalidRequestError, match="积分流水不可"):
            session.commit()
        session.rollback()

        persisted = session.get(CreditLog, "log")
        assert persisted is not None
        assert persisted.note is None
    engine.dispose()


def test_credit_models_declare_database_invariants():
    account_checks = {
        constraint.name
        for constraint in CreditAccount.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }
    reservation_checks = {
        constraint.name
        for constraint in CreditReservation.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }
    log_checks = {
        constraint.name
        for constraint in CreditLog.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }
    reservation_unique_columns = {
        tuple(column.name for column in constraint.columns)
        for constraint in CreditReservation.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    }

    assert {
        "ck_credit_accounts_balance_nonnegative",
        "ck_credit_accounts_frozen_nonnegative",
        "ck_credit_accounts_frozen_within_balance",
        "ck_credit_accounts_owner_type",
        "ck_credit_accounts_owner_scope",
        "ck_credit_accounts_status",
    } <= account_checks
    assert {
        "ck_credit_reservations_points_positive",
        "ck_credit_reservations_actual_nonnegative",
        "ck_credit_reservations_actual_by_status",
        "ck_credit_reservations_status",
    } <= reservation_checks
    assert {
        "ck_credit_logs_log_type",
        "ck_credit_logs_effect_shape",
    } <= log_checks
    assert ("source_type", "source_id") in reservation_unique_columns


def test_credit_database_constraints_reject_inconsistent_financial_rows(tmp_path):
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'constraints.sqlite3'}")
    _create_credit_tables(engine)
    with engine.begin() as connection:
        connection.execute(
            insert(CreditAccount).values(
                id="acc",
                owner_type="user",
                owner_id="u1",
                balance_points=100,
                frozen_points=0,
                status="active",
            )
        )

    invalid_rows = [
        insert(CreditAccount).values(
            id="dirty-org",
            owner_type="org",
            owner_id="orgA",
            org_id="orgB",
            balance_points=0,
            frozen_points=0,
            status="active",
        ),
        insert(CreditReservation).values(
            id="captured-without-actual",
            account_id="acc",
            source_type="video",
            source_id="job-1",
            reserved_points=10,
            actual_points=None,
            status="captured",
        ),
        insert(CreditLog).values(
            id="invalid-reserve-log",
            account_id="acc",
            reservation_id=None,
            log_type="reserve",
            points_delta=1,
            frozen_delta=0,
            balance_after=100,
            frozen_after=0,
        ),
    ]
    for statement in invalid_rows:
        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(statement)
    engine.dispose()
