import struct

import pymysql
import socket
import logging
import time
import datetime

"""
Author: 李航
Email: hangli_work@163.com
Date: 2024-09-23
Description:
    本脚本用于接收含光门遗址的风速（平均风速，风向）和土壤（湿度，温度）数据，
    并将数据存储在数据库hanguang中
"""

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 数据库连接参数
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'Hanguang@911',
    'database': 'sensor_test'
}

# 监听端口
LISTEN_PORT = 59666
# 超时时间（秒）
TIMEOUT_DURATION = 10
SOIL_DATA_LENGTH = 23  # 土壤数据长度
WIND_DATA_LENGTH = 22  # 风速数据长度
RAIN_DATA_LENGTH = 26  # 雨量数据长度


def create_database_connection():
    """创建数据库连接"""
    return pymysql.connect(**DB_CONFIG)


def listen_for_connections(sock):
    """监听连接并处理数据"""
    while True:
        logging.info('Waiting for a connection...')
        connection_socket, client_address = sock.accept()
        try:
            logging.info('Connection from: %s', client_address)
            connection_socket.settimeout(TIMEOUT_DURATION)
            process_connection(connection_socket)
        except Exception as e:
            logging.error("Error processing connection: %s", str(e))
        finally:
            connection_socket.close()


def process_connection(connection_socket):
    """处理与客户端的连接"""
    last_received_time = time.time()

    while True:
        try:
            data = connection_socket.recv(1024)
            if not data:
                break  # 客户端断开连接
            logging.info('Received: %s', data)

            process_received_data(data, connection_socket)
            last_received_time = time.time()  # 更新最后接收时间

        except socket.timeout:
            if time.time() - last_received_time > TIMEOUT_DURATION:
                logging.info("No data received for %d seconds. Closing connection.", TIMEOUT_DURATION)
                break  # 超时，关闭连接
        except Exception as e:
            logging.error("Error during data reception: %s", str(e))
            break


def process_received_data(data, connection_socket):
    """处理接收到的数据"""
    try:
        # 将字节数据转换为16进制字符串
        if isinstance(data, bytes):
            hex_string = data.hex()  # 将字节数据转换为16进制字符串
        else:
            hex_string = data.strip()  # 如果已经是字符串，直接去除空白符

        # 保留原始16进制数据
        hex_values = ['0'] + [hex_string[i:i + 2] for i in range(0, len(hex_string), 2)]  # 每两个字符一组

        # 打印 hex_values 数组
        logging.info('hex_values: %s', ' '.join(hex_values))

        # 判断帧头标志
        if hex_values[1] != '23':  # 检查是否为 0x23
            logging.warning("Invalid frame header: %s", hex_string)
            return  # 丢弃这一条数据

        # 传感器类型
        sensor_type = hex_values[2]

        # 风速数据
        if sensor_type == '01':

            # 检查数据长度是否符合要求
            if len(hex_values) != WIND_DATA_LENGTH + 1:  # 根据实际数据结构调整这个值
                logging.warning("Invalid data length: Expected %d elements, got %d elements", WIND_DATA_LENGTH,
                                len(hex_values))
                return  # 丢弃这条数据

            gateway_address = hex_values[3]
            terminal_address = int(hex_values[4], 16)
            sensor_address = int(hex_values[5], 16)
            data_length = int(hex_values[6], 16)
            avg_speed_3s = int(hex_values[7] + hex_values[8], 16) / 100.0
            wind_direction_3s = int(hex_values[9] + hex_values[10], 16)
            avg_speed_2min = int(hex_values[11] + hex_values[12], 16) / 100.0
            wind_direction_2min = int(hex_values[13] + hex_values[14], 16)
            avg_speed_10min = int(hex_values[15] + hex_values[16], 16) / 100.0
            wind_direction_10min = int(hex_values[17] + hex_values[18], 16)
            battery = float(int(hex_values[19], 16))
            status = int(hex_values[20], 16)
            checksum = int(hex_values[21], 16)
            frame_tail = hex_values[22]

            # 检查帧尾是否正常
            if frame_tail != '21':
                logging.warning("Invalid frame tail: %s", data)
                return

            # 获取当前时间戳
            unix_timestamp = int(time.time())

            formatted_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # 确定插入的表名
            table_name = f"wind{terminal_address:02d}{sensor_address:02d}"

            conn = create_database_connection()
            try:
                with conn.cursor() as cursor:
                    # 检查表是否存在
                    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                    result = cursor.fetchone()

                    if result is None:
                        logging.warning(f"Table '{table_name}' does not exist. Creating table...")

                        cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
                        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")

                        create_table_query = (
                            f"CREATE TABLE `{table_name}` (\n"
                            f"  `id` varchar(255) NOT NULL,\n"
                            f"  `datetime` datetime DEFAULT NULL,\n"
                            f"  `avg_speed_3s` double DEFAULT NULL,\n"
                            f"  `wind_direction_3s` double DEFAULT NULL,\n"
                            f"  `avg_speed_2m` double DEFAULT NULL,\n"
                            f"  `wind_direction_2m` double DEFAULT NULL,\n"
                            f"  `avg_speed_10m` double DEFAULT NULL,\n"
                            f"  `wind_direction_10m` double DEFAULT NULL,\n"
                            f"  `battery` float DEFAULT NULL,\n"
                            f"  `status` tinyint(4) DEFAULT NULL,\n"
                            f"  PRIMARY KEY (`id`)\n"
                            f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
                        )
                        cursor.execute(create_table_query)
                        conn.commit()
                        logging.info(f"Table '{table_name}' created successfully.")

                    # 插入数据
                    insert_query = (
                        f"INSERT INTO `{table_name}` "
                        "(id, datetime, avg_speed_3s, wind_direction_3s, "
                        "avg_speed_2m, wind_direction_2m, avg_speed_10m, "
                        "wind_direction_10m, battery, status) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    )
                    cursor.execute(insert_query, (
                        str(unix_timestamp), formatted_datetime,
                        float(avg_speed_3s), float(wind_direction_3s),
                        float(avg_speed_2min), float(wind_direction_2min),
                        float(avg_speed_10min), float(wind_direction_10min),
                        battery, status
                    ))
                    conn.commit()
                    logging.info(f"Data inserted into table '{table_name}' successfully.")
            except Exception as e:
                logging.error(f"Unexpected error: {e}")
            finally:
                conn.close()

        # 土壤数据（不完善，暂不考虑）
        elif sensor_type == '02':
            gateway_address = hex_values[3]
            terminal_address = hex_values[4]

            sensor_address1 = int(hex_values[5], 16)
            humidity1 = int(hex_values[6] + hex_values[7], 16)
            temperature1 = int(hex_values[8] + hex_values[9], 16)

            sensor_address2 = int(hex_values[10], 16)
            humidity2 = int(hex_values[11] + hex_values[12], 16)
            temperature2 = int(hex_values[13] + hex_values[14], 16)

            sensor_address3 = int(hex_values[15], 16)
            humidity3 = int(hex_values[16] + hex_values[17], 16)
            temperature3 = int(hex_values[18] + hex_values[19], 16)

            battery = float(int(hex_values[20], 16))
            status = int(hex_values[21], 16)
            checksum = int(hex_values[22], 16)
            frame_tail = hex_values[23]

            return

        # 雨量数据
        elif sensor_type == '03':
            # 检查数据长度是否符合要求
            if len(hex_values) != RAIN_DATA_LENGTH + 1:  # 根据实际数据结构调整这个值
                logging.warning("Invalid data length: Expected %d elements, got %d elements", RAIN_DATA_LENGTH,
                                len(hex_values))
                return  # 丢弃这条数据

            gateway_address = hex_values[3]
            terminal_address = int(hex_values[4], 16)
            sensor_address = int(hex_values[5], 16)
            data_length = int(hex_values[6], 16)

            average_rainfall_per_minute = ieee754_binary32_to_float(hex_values_to_binary(hex_values, 7, 10))
            average_rainfall_per_hour = ieee754_binary32_to_float(hex_values_to_binary(hex_values, 11, 14))
            average_rainfall_per_day = ieee754_binary32_to_float(hex_values_to_binary(hex_values, 15, 18))
            total_rainfall = ieee754_binary32_to_float(hex_values_to_binary(hex_values, 19, 22))
            battery = int(hex_values[23], 16)
            status = int(hex_values[24], 16)
            checksum = int(hex_values[25], 16)
            frame_tail = hex_values[26]

            # 检查帧尾是否正常
            if frame_tail != '21':
                logging.warning("Invalid frame tail: %s", data)
                return

            print(
                f"Gateway Address: {gateway_address}, "
                f"Terminal Address: {terminal_address}, "
                f"Sensor Address: {sensor_address}, "
                f"Data Length: {data_length}, "
                f"Average Rainfall per Minute: {average_rainfall_per_minute}, "
                f"Average Rainfall per Hour: {average_rainfall_per_hour}, "
                f"Average Rainfall per Day: {average_rainfall_per_day}, "
                f"Total Rainfall: {total_rainfall}, Battery: {battery}, "
                f"Status: {status}, Checksum: {checksum}, "
                f"Frame Tail: {frame_tail}")


        else:
            logging.warning("Unknown sensor type: %s", sensor_type)
            return


    except ValueError as ve:
        logging.error("Data format error: %s", str(ve))
        connection_socket.sendall(b"Invalid data format")


def ieee754_binary32_to_float(binary_str):
    """
    将32位IEEE754二进制数转换为浮点数。

    参数:
        binary_str (str): 32位二进制字符串

    返回:
        float: 对应的浮点数
    """
    if len(binary_str) != 32:
        raise ValueError("输入必须是32位二进制字符串")

    # 将二进制字符串转换为整数
    integer_representation = int(binary_str, 2)

    # 使用 struct 模块将整数转换为浮点数
    packed = struct.pack('I', integer_representation)
    return struct.unpack('f', packed)[0]


def hex_values_to_binary(hex_values, start_index, end_index):
    """
    将给定的16进制值列表中的指定索引范围内的值转换为二进制字符串。

    参数:
        hex_values (list): 16进制值列表
        start_index (int): 开始索引（包含）
        end_index (int): 结束索引（包含）

    返回:
        str: 二进制字符串
    """
    if len(hex_values) <= end_index:
        raise ValueError(f"hex_values 列表长度必须大于 {end_index}")

    # 提取指定索引范围内的值（包含 end_index）
    selected_hex_values = hex_values[start_index:end_index + 1]

    # 计算总位数
    total_bits = (end_index - start_index + 1) * 8

    # 将这些16进制值合并成一个整数
    combined_hex = ''.join(selected_hex_values)
    combined_int = int(combined_hex, 16)

    # 将这个整数转换为二进制字符串
    binary_string = format(combined_int, f'0{total_bits}b')

    return binary_string


def run_server():
    # 创建监听socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('0.0.0.0', LISTEN_PORT)
    sock.bind(server_address)
    sock.listen(1)

    # 监听连接
    listen_for_connections(sock)


def main():
    run_server()


if __name__ == "__main__":
    main()
