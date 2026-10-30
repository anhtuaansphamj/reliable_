import argparse
import socket
import sys
import time

from utils import PacketHeader, compute_checksum


HEADER_SIZE = 16
MAX_PACKET_SIZE = 1472
MAX_PAYLOAD_SIZE = MAX_PACKET_SIZE - HEADER_SIZE

START = 0
END = 1
DATA = 2
ACK = 3

TIMEOUT = 0.5  # 500 ms


def make_packet(pkt_type, seq_num, payload=b""):
    """Create an RTP packet with a valid checksum."""
    header = PacketHeader(
        type=pkt_type,
        seq_num=seq_num,
        length=len(payload),
        checksum=0
    )

    # Compute checksum while the checksum field is zero.
    checksum = compute_checksum(header / payload)
    header.checksum = checksum

    return bytes(header / payload)


def parse_ack(pkt):
    """Parse and validate an ACK packet. Return its sequence number or None."""
    if len(pkt) != HEADER_SIZE:
        return None

    try:
        header = PacketHeader(pkt)

        if header.type != ACK or header.length != 0:
            return None

        received_checksum = header.checksum
        header.checksum = 0

        if received_checksum != compute_checksum(header):
            return None

        return header.seq_num

    except Exception:
        return None


def wait_for_start_ack(s, receiver_address):
    """Send START and wait for its ACK."""
    start_packet = make_packet(START, 0)

    # The project specification does not require handling
    # a delayed or dropped START ACK.
    s.sendto(start_packet, receiver_address)
    s.settimeout(TIMEOUT)

    while True:
        try:
            pkt, address = s.recvfrom(2048)
            print("DEBUG received packet from:", address)
            print("DEBUG expected address:", receiver_address)
        except socket.timeout:
            # Retry to make startup more robust.
            s.sendto(start_packet, receiver_address)
            continue

        if address != receiver_address:
            continue

        ack_seq = parse_ack(pkt)

        if ack_seq == 1:
            return


def wait_for_end_ack(s, receiver_address, end_seq):
    """Send END and retry until its ACK is received."""
    end_packet = make_packet(END, end_seq)
    s.settimeout(TIMEOUT)

    while True:
        s.sendto(end_packet, receiver_address)

        deadline = time.monotonic() + TIMEOUT

        while True:
            remaining = deadline - time.monotonic()

            if remaining <= 0:
                break

            s.settimeout(remaining)

            try:
                pkt, address = s.recvfrom(2048)
            except socket.timeout:
                break

            if address != receiver_address:
                continue

            ack_seq = parse_ack(pkt)

            if ack_seq == end_seq:
                return


def sender(receiver_ip, receiver_port, window_size):
    """Send data reliably using individual ACKs."""

    if window_size <= 0:
        raise ValueError("window_size must be positive")

    receiver_address = (socket.gethostbyname(receiver_ip), receiver_port)

    # Đọc dữ liệu đầu vào dưới dạng bytes.
    message = sys.stdin.buffer.read()

    # Chia dữ liệu thành các gói DATA.
    chunks = [
        message[i:i + MAX_PAYLOAD_SIZE]
        for i in range(0, len(message), MAX_PAYLOAD_SIZE)
    ]

    packets = {
        seq: make_packet(DATA, seq, chunk)
        for seq, chunk in enumerate(chunks, start=1)
    }

    total_packets = len(chunks)

    # END nằm ngay sau gói DATA cuối cùng.
    end_seq = total_packets + 1

    # base: gói đầu tiên chưa được ACK.
    base = 1

    # next_seq: số thứ tự tiếp theo cần gửi.
    next_seq = 1

    # Theo dõi ACK riêng của từng gói.
    acked = [False] * (total_packets + 1)

    timer_start = None

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        wait_for_start_ack(s, receiver_address)

        while base <= total_packets:

            # Gửi các gói mới trong cửa sổ.
            while (
                next_seq <= total_packets
                and next_seq < base + window_size
            ):
                s.sendto(packets[next_seq], receiver_address)

                if timer_start is None:
                    timer_start = time.monotonic()

                next_seq += 1

            # Đảm bảo các gói chưa ACK có timer.
            if timer_start is None:
                timer_start = time.monotonic()

            remaining = TIMEOUT - (
                time.monotonic() - timer_start
            )

            # Hết thời gian: chỉ gửi lại các gói chưa ACK.
            if remaining <= 0:
                for seq in range(base, next_seq):
                    if not acked[seq]:
                        s.sendto(packets[seq], receiver_address)

                timer_start = time.monotonic()
                continue

            s.settimeout(remaining)

            try:
                pkt, address = s.recvfrom(2048)
            except socket.timeout:
                continue

            if address != receiver_address:
                continue

            ack_seq = parse_ack(pkt)

            # ACK không hợp lệ hoặc nằm ngoài các gói đang gửi.
            if ack_seq is None:
                continue

            if not (base <= ack_seq < next_seq):
                continue

            # Xác nhận riêng gói này.
            acked[ack_seq] = True

            old_base = base

            # Dịch cửa sổ qua các gói đã được ACK liên tiếp.
            while (
                base <= total_packets
                and acked[base]
            ):
                base += 1

            # Reset timer khi gói đầu tiên chưa ACK thay đổi.
            if base != old_base:
                timer_start = (
                    time.monotonic()
                    if base < next_seq
                    else None
                )

        # Tất cả DATA đã được ACK riêng.
        wait_for_end_ack(s, receiver_address, end_seq)

    finally:
        s.close()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "receiver_ip",
        help="The IP address of the host that receiver is running on"
    )
    parser.add_argument(
        "receiver_port",
        type=int,
        help="The port number on which receiver is listening"
    )
    parser.add_argument(
        "window_size",
        type=int,
        help="Maximum number of outstanding packets"
    )

    args = parser.parse_args()

    sender(
        args.receiver_ip,
        args.receiver_port,
        args.window_size
    )


if __name__ == "__main__":
    main()
