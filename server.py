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
    'password': 'Xt12345678.',
    'database': 'hanguang'
}

# 监听端口
LISTEN_PORT = 59666
# 超时时间（秒）
TIMEOUT_DURATION = 10
WIND_DATA_LENGTH = 23  # 风速数据长度

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
        hex_values = ['0'] + [hex_string[i:i+2] for i in range(0, len(hex_string), 2)]  # 每两个字符一组

        # 打印 hex_values 数组
        logging.info('hex_values: %s', ' '.join(hex_values))
        
        # 检查数据长度是否符合要求
        if len(hex_values) != WIND_DATA_LENGTH:  # 根据实际数据结构调整这个值
            logging.warning("Invalid data length: Expected 23 elements, got %d elements", len(hex_values))
            return  # 丢弃这条数据

        # 判断帧头标志
        if hex_values[1] != '23':  # 检查是否为 0x23
            logging.warning("Invalid frame header: %s", hex_string)
            return  # 丢弃这一条数据
            
        # 传感器类型
        sensor_type = hex_values[2]
        
        # 风速数据
        if sensor_type == '01':
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
                logging.warning("Invalid frame tail: %s", original_hex_data)
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

            except mysql.connector.Error as err:
                logging.error(f"Database error: {err}")
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
            

        else:
            logging.warning("Unknown sensor type: %s", sensor_type)
            return
        
        
    except ValueError as ve:
        logging.error("Data format error: %s", str(ve))
        connection_socket.sendall(b"Invalid data format")

def main():
    # 创建监听socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('0.0.0.0', LISTEN_PORT)
    sock.bind(server_address)
    sock.listen(1)
    
    # 监听连接
    listen_for_connections(sock)

if __name__ == "__main__":
    main()
