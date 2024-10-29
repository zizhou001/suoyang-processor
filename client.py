import binascii
import socket
import time
import random

# 服务器地址和端口
server_address = ('localhost', 59666)


def create_client_socket():
    """创建并返回一个客户端socket"""
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(server_address)
    return client_socket


def send_wind_data(client_socket):
    """生成并发送风速数据"""
    while True:
        # 生成随机数据
        sensor_type = '01'  # 风速传感器类型
        gateway_address = '01'  # 示例网关地址
        terminal_address = '01'  # 示例终端地址
        sensor_address = random.randint(1, 2)  # 

        # 生成风速数据
        avg_speed_3s = random.randint(0, 300)  # 0-300
        wind_direction_3s = random.randint(0, 360)  # 0-360
        avg_speed_2min = random.randint(0, 300)  # 0-300
        wind_direction_2min = random.randint(0, 360)  # 0-360
        avg_speed_10min = random.randint(0, 300)  # 0-300
        wind_direction_10min = random.randint(0, 360)  # 0-360
        device_battery = random.randint(0, 100)  # 0-100%
        device_status = random.randint(0, 1)  # 0 或 1

        # 生成风速数据长度（注意第7字节为数据有效长度）
        data_length = 21  # 实际数据长度，包含校验和和帧尾

        # 格式化为16进制字符串
        hex_data = (
            f"23 {sensor_type} {gateway_address} {terminal_address} "
            f"{sensor_address:02X} {data_length:02X} "
            f"{avg_speed_3s // 100:02X} {avg_speed_3s % 100:02X} "
            f"{wind_direction_3s // 100:02X} {wind_direction_3s % 100:02X} "
            f"{avg_speed_2min // 100:02X} {avg_speed_2min % 100:02X} "
            f"{wind_direction_2min // 100:02X} {wind_direction_2min % 100:02X} "
            f"{avg_speed_10min // 100:02X} {avg_speed_10min % 100:02X} "
            f"{wind_direction_10min // 100:02X} {wind_direction_10min % 100:02X} "
            f"{device_battery:02X} {device_status:02X} "
            "00 21"  # 校验和和帧尾
        )

        print(f"Sending: {hex_data}")
        client_socket.sendall(hex_data.encode())
        time.sleep(5)  # 每5秒发送一次数据


def send_rainfall_data(client_socket):
    hex_data = '2303567890abcdef1234567890abcdef1234567890abcdef1221'

    # 将16进制字符串转换为字节序列
    byte_data = binascii.unhexlify(hex_data)

    # 发送字节序列
    client_socket.sendall(byte_data)
    time.sleep(10)


def main():
    client_socket = create_client_socket()
    try:
        send_rainfall_data(client_socket)
    except KeyboardInterrupt:
        print("Client stopped.")
    finally:
        client_socket.close()


if __name__ == "__main__":
    main()
