"""SVG 确定性封面生成 —— 按标题生成 3 段线性渐变 + 高斯模糊占位图。

零存储：直接返回 ``data:image/svg+xml`` 内联 URI。
同一标题生成的封面确定且可复现 (基于标题 hash 选色)。
"""

from __future__ import annotations

import hashlib
import urllib.parse


def _hash_title(title: str) -> int:
    """对标题做稳定哈希 (md5 取前 8 字节为 int)。"""
    h = hashlib.md5(title.encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big")


def _hsl_to_rgb(h: float, s: float, lightness: float) -> tuple[int, int, int]:
    """HSL → RGB (0-255)。"""
    import colorsys

    r, g, b = colorsys.hls_to_rgb(h, lightness, s)
    return int(r * 255), int(g * 255), int(b * 255)


def _pick_palette(seed: int) -> list[tuple[int, int, int]]:
    """基于 seed 确定性选取 3 段渐变色。"""
    palette: list[tuple[int, int, int]] = []
    for i in range(3):
        # 每段独立取色相，保证视觉差异
        hue = ((seed >> (i * 11)) & 0xFFF) / 0xFFF  # 0~1
        sat = 0.55 + ((seed >> (i * 5 + 3)) & 0x1F) / 0x1F * 0.25  # 0.55~0.8
        lig = 0.42 + ((seed >> (i * 7 + 1)) & 0x1F) / 0x1F * 0.18  # 0.42~0.6
        palette.append(_hsl_to_rgb(hue, sat, lig))
    return palette


def _rgb_hex(rgb: tuple[int, int, int]) -> str:
    return "#{:02x}{:02x}{:02x}".format(*rgb)


def generate_svg_cover(title: str, width: int = 480, height: int = 270) -> str:
    """按标题确定性生成 3 段线性渐变 + 高斯模糊 SVG 封面。

    返回 ``data:image/svg+xml,...`` 内联 URI (URL 编码)。
    """
    seed = _hash_title(title or "default")
    palette = _pick_palette(seed)

    stops = "\n".join(
        f'      <stop offset="{i / (len(palette) - 1):.3f}" stop-color="{_rgb_hex(c)}" />'
        for i, c in enumerate(palette)
    )

    # 高斯模糊让色块过渡柔和，模拟占位封面质感
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
  <defs>
    <linearGradient id="g{seed & 0xFFFFFF:x}" x1="0%" y1="0%" x2="100%" y2="100%">
{stops}
    </linearGradient>
    <filter id="b{seed & 0xFFFFFF:x}">
      <feGaussianBlur stdDeviation="24" />
    </filter>
  </defs>
  <rect width="{width}" height="{height}" fill="url(#g{seed & 0xFFFFFF:x})" filter="url(#b{seed & 0xFFFFFF:x})" />
</svg>"""

    encoded = urllib.parse.quote(svg, safe="")
    return f"data:image/svg+xml,{encoded}"
