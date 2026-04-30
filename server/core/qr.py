from __future__ import annotations

from typing import Iterable


_EXP = [0] * 512
_LOG = [0] * 256
value = 1
for index in range(255):
    _EXP[index] = value
    _LOG[value] = index
    value <<= 1
    if value & 0x100:
        value ^= 0x11D
for index in range(255, 512):
    _EXP[index] = _EXP[index - 255]


def _gf_mul(left: int, right: int) -> int:
    if left == 0 or right == 0:
        return 0
    return _EXP[_LOG[left] + _LOG[right]]


def _reed_solomon_generator(degree: int) -> list[int]:
    result = [1]
    for index in range(degree):
        next_result = [0] * (len(result) + 1)
        for item, coefficient in enumerate(result):
            next_result[item] ^= coefficient
            next_result[item + 1] ^= _gf_mul(coefficient, _EXP[index])
        result = next_result
    return result[1:]


def _reed_solomon_remainder(data: list[int], degree: int) -> list[int]:
    generator = _reed_solomon_generator(degree)
    result = [0] * degree
    for byte in data:
        factor = byte ^ result.pop(0)
        result.append(0)
        for index, coefficient in enumerate(generator):
            if factor:
                result[index] ^= _gf_mul(coefficient, factor)
    return result


def _append_bits(buffer: list[int], value: int, length: int) -> None:
    for index in range(length - 1, -1, -1):
        buffer.append((value >> index) & 1)


def _build_codewords(text: str) -> list[int]:
    data = text.encode("utf-8")
    if len(data) > 134:
        raise ValueError("QR payload is too long for version 6-L")

    bits: list[int] = []
    _append_bits(bits, 0b0100, 4)
    _append_bits(bits, len(data), 8)
    for byte in data:
        _append_bits(bits, byte, 8)

    capacity_bits = 136 * 8
    _append_bits(bits, 0, min(4, capacity_bits - len(bits)))
    while len(bits) % 8:
        bits.append(0)

    codewords = [sum(bit << (7 - index) for index, bit in enumerate(bits[offset : offset + 8])) for offset in range(0, len(bits), 8)]
    pad = 0xEC
    while len(codewords) < 136:
        codewords.append(pad)
        pad = 0x11 if pad == 0xEC else 0xEC

    blocks = [codewords[:68], codewords[68:]]
    ecc_blocks = [_reed_solomon_remainder(block, 18) for block in blocks]
    interleaved: list[int] = []
    for index in range(68):
        interleaved.extend(block[index] for block in blocks)
    for index in range(18):
        interleaved.extend(block[index] for block in ecc_blocks)
    return interleaved


def _empty_matrix(size: int) -> tuple[list[list[bool]], list[list[bool]]]:
    return ([[False] * size for _ in range(size)], [[False] * size for _ in range(size)])


def _set_module(matrix: list[list[bool]], reserved: list[list[bool]], x: int, y: int, value: bool, *, reserve: bool = True) -> None:
    if 0 <= x < len(matrix) and 0 <= y < len(matrix):
        matrix[y][x] = value
        if reserve:
            reserved[y][x] = True


def _finder(matrix: list[list[bool]], reserved: list[list[bool]], x: int, y: int) -> None:
    for dy in range(-1, 8):
        for dx in range(-1, 8):
            xx = x + dx
            yy = y + dy
            if not (0 <= xx < len(matrix) and 0 <= yy < len(matrix)):
                continue
            value = 0 <= dx <= 6 and 0 <= dy <= 6 and (dx in {0, 6} or dy in {0, 6} or (2 <= dx <= 4 and 2 <= dy <= 4))
            _set_module(matrix, reserved, xx, yy, value)


def _alignment(matrix: list[list[bool]], reserved: list[list[bool]], center_x: int, center_y: int) -> None:
    for dy in range(-2, 3):
        for dx in range(-2, 3):
            value = max(abs(dx), abs(dy)) != 1
            _set_module(matrix, reserved, center_x + dx, center_y + dy, value)


def _reserve_format(matrix: list[list[bool]], reserved: list[list[bool]]) -> None:
    size = len(matrix)
    for index in range(9):
        _set_module(matrix, reserved, 8, index, matrix[index][8])
        _set_module(matrix, reserved, index, 8, matrix[8][index])
        _set_module(matrix, reserved, size - 1 - index, 8, matrix[8][size - 1 - index])
        _set_module(matrix, reserved, 8, size - 1 - index, matrix[size - 1 - index][8])


def _draw_function_patterns() -> tuple[list[list[bool]], list[list[bool]]]:
    size = 41
    matrix, reserved = _empty_matrix(size)
    _finder(matrix, reserved, 0, 0)
    _finder(matrix, reserved, size - 7, 0)
    _finder(matrix, reserved, 0, size - 7)
    _alignment(matrix, reserved, 34, 34)
    for index in range(8, size - 8):
        value = index % 2 == 0
        _set_module(matrix, reserved, index, 6, value)
        _set_module(matrix, reserved, 6, index, value)
    _set_module(matrix, reserved, 8, 33, True)
    _reserve_format(matrix, reserved)
    return matrix, reserved


def _place_data(matrix: list[list[bool]], reserved: list[list[bool]], codewords: Iterable[int]) -> None:
    bits = [(byte >> shift) & 1 == 1 for byte in codewords for shift in range(7, -1, -1)]
    bit_index = 0
    size = len(matrix)
    upward = True
    x = size - 1
    while x > 0:
        if x == 6:
            x -= 1
        rows = range(size - 1, -1, -1) if upward else range(size)
        for y in rows:
            for dx in (0, 1):
                xx = x - dx
                if reserved[y][xx]:
                    continue
                matrix[y][xx] = bits[bit_index] if bit_index < len(bits) else False
                bit_index += 1
        upward = not upward
        x -= 2


def _mask(mask: int, x: int, y: int) -> bool:
    return (
        (x + y) % 2 == 0
        if mask == 0
        else y % 2 == 0
        if mask == 1
        else x % 3 == 0
        if mask == 2
        else (x + y) % 3 == 0
        if mask == 3
        else (x // 3 + y // 2) % 2 == 0
        if mask == 4
        else (x * y) % 2 + (x * y) % 3 == 0
        if mask == 5
        else ((x * y) % 2 + (x * y) % 3) % 2 == 0
        if mask == 6
        else ((x + y) % 2 + (x * y) % 3) % 2 == 0
    )


def _masked(matrix: list[list[bool]], reserved: list[list[bool]], mask: int) -> list[list[bool]]:
    size = len(matrix)
    result = [row[:] for row in matrix]
    for y in range(size):
        for x in range(size):
            if not reserved[y][x] and _mask(mask, x, y):
                result[y][x] = not result[y][x]
    return result


def _format_bits(mask: int) -> int:
    data = (0b01 << 3) | mask
    bits = data << 10
    generator = 0x537
    for shift in range(14, 9, -1):
        if (bits >> shift) & 1:
            bits ^= generator << (shift - 10)
    return ((data << 10) | bits) ^ 0x5412


def _draw_format(matrix: list[list[bool]], mask: int) -> None:
    size = len(matrix)
    bits = _format_bits(mask)
    for index in range(15):
        value = ((bits >> index) & 1) == 1
        if index < 6:
            matrix[index][8] = value
        elif index < 8:
            matrix[index + 1][8] = value
        else:
            matrix[size - 15 + index][8] = value

        if index < 8:
            matrix[8][size - 1 - index] = value
        else:
            matrix[8][14 - index] = value
    matrix[33][8] = True


def _run_penalty(line: list[bool]) -> int:
    total = 0
    run_color = line[0]
    run_length = 1
    for value in line[1:]:
        if value == run_color:
            run_length += 1
            continue
        if run_length >= 5:
            total += 3 + run_length - 5
        run_color = value
        run_length = 1
    if run_length >= 5:
        total += 3 + run_length - 5
    return total


def _penalty(matrix: list[list[bool]]) -> int:
    size = len(matrix)
    total = sum(_run_penalty(row) for row in matrix)
    total += sum(_run_penalty([matrix[y][x] for y in range(size)]) for x in range(size))
    for y in range(size - 1):
        for x in range(size - 1):
            color = matrix[y][x]
            if matrix[y][x + 1] == color and matrix[y + 1][x] == color and matrix[y + 1][x + 1] == color:
                total += 3
    dark = sum(1 for row in matrix for value in row if value)
    total += abs(dark * 20 - size * size * 10) // (size * size) * 10
    return total


def qr_svg(text: str, *, scale: int = 5, border: int = 4) -> str:
    base, reserved = _draw_function_patterns()
    _place_data(base, reserved, _build_codewords(text))
    choices = []
    for mask in range(8):
        candidate = _masked(base, reserved, mask)
        _draw_format(candidate, mask)
        choices.append((_penalty(candidate), candidate))
    matrix = min(choices, key=lambda item: item[0])[1]
    size = len(matrix)
    side = (size + border * 2) * scale
    rects = []
    for y, row in enumerate(matrix):
        for x, value in enumerate(row):
            if value:
                rects.append(f'<rect x="{(x + border) * scale}" y="{(y + border) * scale}" width="{scale}" height="{scale}"/>')
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {side} {side}" width="{side}" height="{side}" role="img">'
        f'<rect width="100%" height="100%" fill="#fff"/>'
        f'<g fill="#111827">{"".join(rects)}</g>'
        f"</svg>"
    )
