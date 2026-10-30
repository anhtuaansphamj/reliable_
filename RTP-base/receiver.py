import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum


HEADER_SIZE = 16
MAX_PAYLOAD_SIZE = 1456
START = 0
END = 1
DATA = 2
ACK = 3


def make_ack(seq_num):
    """Tạo gói ACK có checksum hợp lệ."""
    ack_header = PacketHeader(
        type=ACK,
        seq_num=seq_num,
        length=0,
        checksum=0
    )
    ack_header.checksum = compute_checksum(ack_header)
    return bytes(ack_header)


def receiver(receiver_ip, receiver_port, window_size):
    """Receive RTP packets reliably over UDP."""

    if window_size <= 0:
        raise ValueError("window_size must be positive")

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((receiver_ip, receiver_port))

    in_connection = False
    connection_address = None

    # Sequence number của DATA tiếp theo cần nhận.
    expected_seq = 1

    # Lưu các gói đến sai thứ tự.
    buffer = {}

    try:
        while True:
            pkt, address = s.recvfrom(2048)

            # Header không đầy đủ.
            if len(pkt) < HEADER_SIZE:
                continue

            pkt_header = PacketHeader(pkt[:HEADER_SIZE])

            # Kiểm tra độ dài payload khai báo trong header.
            if not (0 <= pkt_header.length <= MAX_PAYLOAD_SIZE):
                continue

            if len(pkt) != HEADER_SIZE + pkt_header.length:
                continue

            msg = pkt[HEADER_SIZE:]

            # Xác minh checksum.
            pkt_checksum = pkt_header.checksum
            pkt_header.checksum = 0

            computed_checksum = compute_checksum(pkt_header / msg)

            if pkt_checksum != computed_checksum:
                continue

            pkt_type = pkt_header.type
            seq_num = pkt_header.seq_num

            if seq_num < 0:
                continue

            # 1. Xử lý START.
            if pkt_type == START:
                # Chỉ chấp nhận START khi chưa có kết nối.
                if not in_connection:
                    if pkt_header.length != 0:
                        continue

                    in_connection = True
                    connection_address = address
                    expected_seq = 1
                    buffer.clear()

                    s.sendto(make_ack(1), connection_address)

                # Bỏ qua START khi đang có kết nối.
                continue

            # Chưa có kết nối thì bỏ các gói khác.
            if not in_connection:
                continue

            # Chỉ nhận gói từ Sender đã khởi tạo kết nối.
            if address != connection_address:
                continue

            # 2. Xử lý DATA.
            if pkt_type == DATA:
                # DATA không được nằm ngoài cửa sổ nhận.
                if seq_num >= expected_seq + window_size:
                    continue

                # Gói cũ: gửi lại ACK hiện tại.
                if seq_num < expected_seq:
                    s.sendto(
                        make_ack(expected_seq),
                        connection_address
                    )
                    continue

                # Lưu gói trong cửa sổ.
                buffer[seq_num] = msg

                # Ghi tất cả gói liên tiếp đúng thứ tự.
                while expected_seq in buffer:
                    data = buffer.pop(expected_seq)

                    sys.stdout.buffer.write(data)
                    sys.stdout.buffer.flush()

                    expected_seq += 1

                # ACK cộng dồn: gói tiếp theo đang mong đợi.
                s.sendto(
                    make_ack(expected_seq),
                    connection_address
                )
                continue

            # 3. Xử lý END.
            if pkt_type == END:
                if pkt_header.length != 0:
                    continue

                # Chỉ kết thúc khi tất cả DATA trước END
                # đã được nhận và chuyển ra đầu ra đúng thứ tự.
                if seq_num != expected_seq or buffer:
                    s.sendto(
                        make_ack(expected_seq),
                        connection_address
                    )
                    continue

                # ACK END phải mang seq_num của END.
                s.sendto(
                    make_ack(seq_num),
                    connection_address
                )
                break

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

    receiver(
        args.receiver_ip,
        args.receiver_port,
        args.window_size
    )


if __name__ == "__main__":
    main()
