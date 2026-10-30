
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

    checksum = compute_checksum(header / payload)
    header.checksum = checksum

    return bytes(header / payload)


def parse_ack(pkt):
    """Validate an ACK packet and return its sequence number."""
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


def same_endpoint(address, expected_address):
    """Compare UDP endpoints without relying on hostname spelling."""
    return (
        address[1] == expected_address[1]
        and socket.gethostbyname(address[0])
        == socket.gethostbyname(expected_address[0])
    )


def wait_for_start_ack(s, receiver_address):
    """Send START and wait for its ACK."""
    start_packet = make_packet(START, 0)
    s.settimeout(TIMEOUT)

    while True:
        print("DEBUG: sending START", flush=True)
        s.sendto(start_packet, receiver_address)

        try:
            pkt, address = s.recvfrom(2048)
        except socket.timeout:
            print("DEBUG: START ACK timeout; retrying", flush=True)
            continue

        print("DEBUG: received START response from", address, flush=True)

        if not same_endpoint(address, receiver_address):
            print("DEBUG: ignored response from unexpected endpoint", flush=True)
            continue

        ack_seq = parse_ack(pkt)
        print("DEBUG: START ACK sequence =", ack_seq, flush=True)

        if ack_seq == 1:
            print("DEBUG: START handshake successful", flush=True)
            return


def wait_for_end_ack(s, receiver_address, end_seq):
    """Send END and wait briefly for its ACK."""
    end_packet = make_packet(END, end_seq)
    deadline = time.monotonic() + TIMEOUT

    while True:
        print(f"DEBUG: sending END seq={end_seq}", flush=True)
        s.sendto(end_packet, receiver_address)

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            print("DEBUG: END wait finished", flush=True)
            return

        s.settimeout(remaining)

        try:
            pkt, address = s.recvfrom(2048)
        except socket.timeout:
            print("DEBUG: END ACK timeout", flush=True)
            return

        if not same_endpoint(address, receiver_address):
            continue

        ack_seq = parse_ack(pkt)
        print("DEBUG: END response ACK =", ack_seq, flush=True)

        if ack_seq == end_seq:
            print("DEBUG: END ACK received", flush=True)
            return


def sender(receiver_ip, receiver_port, window_size):
    """Send stdin reliably using the RTP-base sliding-window protocol."""
    if window_size <= 0:
        raise ValueError("window_size must be positive")

    receiver_address = (receiver_ip, receiver_port)

    message = sys.stdin.buffer.read()

    chunks = [
        message[i:i + MAX_PAYLOAD_SIZE]
        for i in range(0, len(message), MAX_PAYLOAD_SIZE)
    ]

    packets = {
        seq: make_packet(DATA, seq, chunk)
        for seq, chunk in enumerate(chunks, start=1)
    }

    total_packets = len(chunks)
    base = 1
    next_seq = 1
    end_seq = total_packets + 1

    print(
        f"DEBUG: input bytes={len(message)}, DATA packets={total_packets}",
        flush=True
    )

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        wait_for_start_ack(s, receiver_address)

        timer_start = None

        while base <= total_packets:
            # Fill the sending window.
            while (
                next_seq <= total_packets
                and next_seq < base + window_size
            ):
                print(f"DEBUG: sending DATA seq={next_seq}", flush=True)
                s.sendto(packets[next_seq], receiver_address)

                if timer_start is None:
                    timer_start = time.monotonic()

                next_seq += 1

            if timer_start is None:
                timer_start = time.monotonic()

            remaining = TIMEOUT - (time.monotonic() - timer_start)

            if remaining <= 0:
                print(
                    f"DEBUG: timeout; retransmitting DATA "
                    f"{base}..{next_seq - 1}",
                    flush=True
                )

                for seq in range(base, next_seq):
                    s.sendto(packets[seq], receiver_address)

                timer_start = time.monotonic()
                continue

            s.settimeout(remaining)

            try:
                pkt, address = s.recvfrom(2048)
            except socket.timeout:
                continue

            print("DEBUG: received response from", address, flush=True)

            if not same_endpoint(address, receiver_address):
                print("DEBUG: ignored response from unexpected endpoint", flush=True)
                continue

            ack_seq = parse_ack(pkt)
            print("DEBUG: parsed DATA ACK =", ack_seq, flush=True)

            if ack_seq is None:
                continue

            # ACK is cumulative: all DATA sequence numbers < ack_seq
            # are acknowledged.
            if base < ack_seq <= next_seq:
                old_base = base
                base = ack_seq

                print(
                    f"DEBUG: window advanced {old_base} -> {base}",
                    flush=True
                )

                timer_start = (
                    time.monotonic()
                    if base < next_seq
                    else None
                )

        print("DEBUG: all DATA packets acknowledged", flush=True)
        wait_for_end_ack(s, receiver_address, end_seq)

    finally:
        s.close()
        print("DEBUG: sender socket closed", flush=True)


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
