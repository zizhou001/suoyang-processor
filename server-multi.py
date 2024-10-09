import socket
import logging
import time
import threading

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def handle_client(connection, client_address):
    logging.info('客户端已连接：%s', client_address)
    connection.settimeout(10)
    last_receive_time = time.time()

    while True:
        try:
            # 接收数据
            data = connection.recv(1024)
            if data:
                logging.info('接收到的数据：%s', data.decode('utf-8'))
                last_receive_time = time.time()  # 更新最后接收时间
            else:
                logging.warning('没有接收到数据，客户端可能已关闭连接')
                break  # 退出循环

            # 检查超时
            if time.time() - last_receive_time > 10:  # 10秒超时
                logging.warning('超时，关闭连接：%s', client_address)
                break

        except socket.timeout:
            logging.warning('超时，关闭连接：%s', client_address)
            break
        except Exception as e:
            logging.error('处理客户端请求时出错：%s', e)
            break

    # 关闭连接
    connection.close()
    logging.info('连接已关闭：%s', client_address)

def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('0.0.0.0', 59666)

    try:
        server_socket.bind(server_address)
        logging.info('服务器绑定成功，等待客户端连接...')
        server_socket.listen(5)  # 允许最多5个待处理的连接

        while True:
            logging.info('等待客户端连接...')
            connection, client_address = server_socket.accept()

            # 创建新线程处理该客户端连接
            client_thread = threading.Thread(target=handle_client, args=(connection, client_address))
            client_thread.start()

    except Exception as e:
        logging.error('启动服务器时出错：%s', e)
    finally:
        server_socket.close()
        logging.info('服务器套接字已关闭')

if __name__ == "__main__":
    start_server()
