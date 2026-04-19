#!/usr/bin/env python3
"""
STEGO ASSEMBLER
===============
Нюхає вхідний HTTP-трафік (port 80),
витягує заголовки X-Secret-Data,
збирає файл навіть якщо пакети прийшли не по порядку.

Формат заголовка:  X-Secret-Data: 00042:00100:deadbeef
                                   ^seq   ^total ^hex-дані

Запуск:
  sudo python3 assembler.py [вихідний_файл]
  sudo python3 assembler.py received.txt
"""

import sys
import signal
from scapy.all import sniff, IP, TCP, Raw

# ── Константи ──────────────────────────────────────────────────────────────
SNIFF_PORT  = 80
HEADER_NAME = "x-secret-data"   # lowercase — HTTP заголовки case-insensitive


# ── Клас асемблера ─────────────────────────────────────────────────────────
class Assembler:
    def __init__(self, output_file: str):
        self.output_file = output_file
        self.chunks: dict[int, bytes] = {}  # seq → дані
        self.total_expected: int | None = None
        print(f"[*] Слухаємо порт {SNIFF_PORT}, збираємо в '{output_file}'")
        print(f"[*] Ctrl+C — зупинити і зберегти\n")

    # -- callback для кожного перехопленого пакету --
    def analyze(self, pkt) -> None:
        if not (pkt.haslayer(TCP) and pkt.haslayer(Raw)):
            return

        try:
            text = pkt[Raw].load.decode("utf-8", errors="ignore")
        except Exception:
            return

        # Шукаємо наш заголовок серед рядків HTTP
        for line in text.split("\r\n"):
            if line.lower().startswith(HEADER_NAME + ":"):
                self._process_header(line)

    # -- парсинг і зберігання одного заголовка --
    def _process_header(self, line: str) -> None:
        # Формат:  X-Secret-Data: 00042:00100:deadbeef
        value = line.split(":", 1)[1].strip()
        parts = value.split(":", 2)

        if len(parts) != 3:
            print(f"[!] Невірний формат заголовка: {line}")
            return

        seq_str, total_str, hex_data = parts

        try:
            seq   = int(seq_str)
            total = int(total_str)
            data  = bytes.fromhex(hex_data)
        except ValueError as e:
            print(f"[!] Помилка парсингу: {e}  рядок='{line}'")
            return

        # Запам'ятовуємо скільки чанків очікувати
        if self.total_expected is None:
            self.total_expected = total
            print(f"[*] Очікуємо {total} чанків")

        # Дубл? Ігноруємо
        if seq in self.chunks:
            return

        self.chunks[seq] = data
        have = len(self.chunks)
        print(f"[+] Chunk {seq:>5}/{total - 1}  [{have}/{total}]  hex={hex_data}")

        # Якщо зібрали все — зберігаємо і виходимо
        if have == total:
            print(f"\n[✓] Всі {total} чанків отримано!")
            self.save()
            print("[*] Готово.")
            # Зупиняємо sniff через виняток (стандартна практика зі Scapy)
            raise KeyboardInterrupt

    # -- збереження зібраного файлу --
    def save(self) -> None:
        if not self.chunks:
            print("[!] Немає чанків для збереження.")
            return

        sorted_keys  = sorted(self.chunks.keys())
        missing      = [i for i in range(sorted_keys[-1] + 1) if i not in self.chunks]

        if missing:
            print(f"[!] Відсутні чанки: {missing[:20]}{'...' if len(missing) > 20 else ''}")
            print(f"[!] Збережемо те що є ({len(sorted_keys)} з {self.total_expected})")

        with open(self.output_file, "wb") as f:
            for k in sorted_keys:
                f.write(self.chunks[k])

        size = sum(len(self.chunks[k]) for k in sorted_keys)
        print(f"[*] Збережено: {self.output_file}  ({size} байт, {len(sorted_keys)} чанків)")

    # -- статус у будь-який момент --
    def status(self) -> None:
        have  = len(self.chunks)
        total = self.total_expected or "?"
        print(f"\n[*] Статус: {have}/{total} чанків")


# ── Точка входу ────────────────────────────────────────────────────────────
def main():
    output_file = sys.argv[1] if len(sys.argv) > 1 else "received.txt"
    assembler   = Assembler(output_file)

    def shutdown(sig, frame):
        assembler.status()
        assembler.save()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)

    try:
        sniff(
            filter  = f"tcp port {SNIFF_PORT}",
            prn     = assembler.analyze,
            store   = 0,   # не накопичувати пакети в пам'яті
        )
    except KeyboardInterrupt:
        pass  # нормальне завершення


if __name__ == "__main__":
    main()