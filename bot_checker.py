#!/usr/bin/env python3
"""
Утилита для проверки и остановки запущенных экземпляров бота
"""

import psutil
import os
import sys
import signal
from pathlib import Path


def find_bot_processes():
    """Находит все процессы бота"""
    bot_processes = []
    current_pid = os.getpid()
    
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['pid'] == current_pid:
                continue
                
            cmdline = proc.info['cmdline']
            if cmdline and any('run.py' in str(cmd) for cmd in cmdline):
                bot_processes.append(proc)
                
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    
    return bot_processes


def stop_bot_processes():
    """Останавливает все найденные процессы бота"""
    processes = find_bot_processes()
    
    if not processes:
        print("? Запущенных экземпляров бота не найдено")
        return True
    
    print(f"?? Найдено {len(processes)} запущенных процессов бота:")
    
    for proc in processes:
        try:
            print(f"  PID: {proc.pid}, CMD: {' '.join(proc.cmdline())}")
        except:
            print(f"  PID: {proc.pid}")
    
    choice = input("\n? Остановить все процессы? (y/N): ").lower()
    
    if choice in ['y', 'yes']:
        stopped = 0
        for proc in processes:
            try:
                proc.terminate()
                proc.wait(timeout=5)
                print(f"? Процесс {proc.pid} остановлен")
                stopped += 1
            except psutil.TimeoutExpired:
                try:
                    proc.kill()
                    print(f"? Процесс {proc.pid} принудительно завершен")
                    stopped += 1
                except:
                    print(f"? Не удалось остановить процесс {proc.pid}")
            except Exception as e:
                print(f"? Ошибка при остановке процесса {proc.pid}: {e}")
        
        print(f"\n? Остановлено {stopped} процессов")
        return True
    
    return False


def check_ports():
    """Проверяет занятые порты"""
    import socket
    
    ports_to_check = [5432, 6379]  # PostgreSQL, Redis
    
    for port in ports_to_check:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            result = sock.connect_ex(('localhost', port))
            if result == 0:
                print(f"? Порт {port} доступен")
            else:
                print(f"? Порт {port} недоступен")
        except Exception as e:
            print(f"? Ошибка проверки порта {port}: {e}")
        finally:
            sock.close()


def main():
    """Главная функция"""
    print("?? Проверка состояния VK Data Assistant Bot\n")
    
    # Проверяем процессы
    processes = find_bot_processes()
    
    if processes:
        print(f"??  Найдено {len(processes)} запущенных экземпляров бота")
        stop_bot_processes()
    else:
        print("? Конфликтующих процессов не найдено")
    
    print("\n?? Проверка портов сервисов:")
    check_ports()
    
    print("\n?? Теперь можно безопасно запустить бота:")
    print("   python run.py")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n?? Прервано пользователем")
        sys.exit(0)
    except Exception as e:
        print(f"? Ошибка: {e}")
        sys.exit(1)