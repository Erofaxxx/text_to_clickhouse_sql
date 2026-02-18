#!/usr/bin/env python3
"""
Text to ClickHouse SQL Converter
Конвертер текстовых запросов на русском языке в SQL запросы для ClickHouse
"""

import os
import sys
import json
import requests
from dotenv import load_dotenv
import clickhouse_connect

# Load environment variables from .env file
load_dotenv()


class ClickHouseSQLGenerator:
    """Main class for converting natural language to ClickHouse SQL"""
    
    def __init__(self):
        """Initialize the SQL generator with API credentials and database connection"""
        # OpenRouter API configuration
        self.api_key = os.getenv('OPENROUTER_API_KEY')
        if not self.api_key:
            raise ValueError("OPENROUTER_API_KEY not found in environment variables")
        
        self.api_url = "https://openrouter.ai/api/v1/chat/completions"
        self.model = "anthropic/claude-3.5-sonnet"
        
        # ClickHouse configuration
        self.ch_host = os.getenv('CLICKHOUSE_HOST', '').replace('https://', '').replace('http://', '')
        self.ch_port = int(os.getenv('CLICKHOUSE_PORT', '8443'))
        self.ch_user = os.getenv('CLICKHOUSE_USER')
        self.ch_password = os.getenv('CLICKHOUSE_PASSWORD')
        self.ch_database = os.getenv('CLICKHOUSE_DATABASE')
        self.ch_table = os.getenv('CLICKHOUSE_TABLE', 'visits_complete')
        self.ch_ssl_cert = os.getenv('CLICKHOUSE_SSL_CERT_PATH')
        
        # Initialize ClickHouse client
        self.client = None
        self.schema_info = None
        
    def connect_to_clickhouse(self):
        """Establish connection to ClickHouse database"""
        try:
            # Configure SSL settings
            settings = {
                'verify': True
            }
            
            if self.ch_ssl_cert and os.path.exists(self.ch_ssl_cert):
                settings['verify'] = self.ch_ssl_cert
            
            self.client = clickhouse_connect.get_client(
                host=self.ch_host,
                port=self.ch_port,
                username=self.ch_user,
                password=self.ch_password,
                database=self.ch_database,
                secure=True,
                **settings
            )
            
            print("✓ Успешно подключено к ClickHouse базе данных")
            return True
        except Exception as e:
            print(f"✗ Ошибка подключения к ClickHouse")
            print(f"  Проверьте настройки подключения в файле .env")
            return False
    
    def get_table_schema(self):
        """Get table schema information from ClickHouse"""
        if not self.client:
            if not self.connect_to_clickhouse():
                return None
        
        try:
            query = f"DESCRIBE TABLE {self.ch_database}.{self.ch_table}"
            result = self.client.query(query)
            
            schema = []
            for row in result.result_rows:
                schema.append({
                    'name': row[0],
                    'type': row[1],
                })
            
            self.schema_info = schema
            return schema
        except Exception as e:
            print(f"✗ Ошибка при получении схемы таблицы: {e}")
            return None
    
    def generate_sql(self, natural_query):
        """
        Generate SQL query from natural language using OpenRouter API
        
        Args:
            natural_query (str): Natural language query in Russian
            
        Returns:
            str: Generated SQL query or None if failed
        """
        # Get schema if not already loaded
        if not self.schema_info:
            self.get_table_schema()
        
        # Prepare schema information for the prompt
        schema_text = ""
        if self.schema_info:
            schema_text = "\n".join([f"- {col['name']} ({col['type']})" for col in self.schema_info])
        
        # Create prompt for the AI model
        system_prompt = f"""Ты эксперт по SQL и ClickHouse. Твоя задача - преобразовать запросы на русском языке в правильные SQL запросы для ClickHouse.

База данных: {self.ch_database}
Таблица: {self.ch_table}

Схема таблицы:
{schema_text if schema_text else "Схема не доступна"}

ВАЖНО:
1. Возвращай ТОЛЬКО SQL запрос, без объяснений и комментариев
2. Используй правильный синтаксис ClickHouse
3. Не добавляй markdown форматирование (без ```sql или ```)
4. Запрос должен быть готов к выполнению
"""

        user_prompt = f"Создай SQL запрос для следующего запроса на русском языке: {natural_query}"
        
        # Prepare API request
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/Erofaxxx/text_to_clickhouse_sql",
            "X-Title": "Text to ClickHouse SQL",
        }
        
        data = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0.1,
            "max_tokens": 1000
        }
        
        try:
            print("\n⏳ Генерация SQL запроса...")
            response = requests.post(self.api_url, headers=headers, json=data, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            sql_query = result['choices'][0]['message']['content'].strip()
            
            # Clean up the SQL query (remove markdown formatting if present)
            sql_query = sql_query.replace('```sql', '').replace('```', '').strip()
            
            return sql_query
        except requests.exceptions.Timeout:
            print("✗ Превышено время ожидания ответа от API")
            print("  Проверьте подключение к интернету или попробуйте позже")
            return None
        except requests.exceptions.RequestException as e:
            print(f"✗ Ошибка при обращении к API")
            print(f"  Проверьте API ключ и подключение к интернету")
            return None
        except (KeyError, IndexError) as e:
            print(f"✗ Ошибка при парсинге ответа API")
            print(f"  Получен неожиданный формат ответа")
            return None
    
    def execute_query(self, sql_query, limit=10):
        """
        Execute SQL query on ClickHouse
        
        Args:
            sql_query (str): SQL query to execute
            limit (int): Maximum number of rows to return
            
        Returns:
            list: Query results or None if failed
        """
        if not self.client:
            if not self.connect_to_clickhouse():
                return None
        
        try:
            # Add LIMIT if not present and it's a SELECT query
            if 'LIMIT' not in sql_query.upper() and sql_query.strip().upper().startswith('SELECT'):
                sql_query = f"{sql_query} LIMIT {limit}"
            
            result = self.client.query(sql_query)
            return result
        except Exception as e:
            print(f"✗ Ошибка при выполнении запроса: {e}")
            return None
    
    def format_results(self, result):
        """
        Format query results for display
        
        Args:
            result: ClickHouse query result
            
        Returns:
            str: Formatted results
        """
        if not result:
            return "Нет результатов"
        
        try:
            rows = result.result_rows
            columns = result.column_names
            
            if not rows:
                return "Запрос выполнен успешно. Результатов: 0"
            
            # Calculate column widths
            col_widths = [len(str(col)) for col in columns]
            for row in rows:
                for i, val in enumerate(row):
                    col_widths[i] = max(col_widths[i], len(str(val)))
            
            # Format header
            header = " | ".join([str(col).ljust(col_widths[i]) for i, col in enumerate(columns)])
            separator = "-+-".join(["-" * w for w in col_widths])
            
            # Format rows
            formatted_rows = []
            for row in rows:
                formatted_row = " | ".join([str(val).ljust(col_widths[i]) for i, val in enumerate(row)])
                formatted_rows.append(formatted_row)
            
            output = [
                "",
                header,
                separator,
                *formatted_rows,
                "",
                f"Всего строк: {len(rows)}"
            ]
            
            return "\n".join(output)
        except Exception as e:
            return f"Ошибка форматирования результатов: {e}"


def print_banner():
    """Print application banner"""
    banner = """
╔══════════════════════════════════════════════════════════════╗
║   Text to ClickHouse SQL - AI-Powered Query Generator       ║
║   Конвертер текстовых запросов в SQL для ClickHouse         ║
╚══════════════════════════════════════════════════════════════╝
"""
    print(banner)


def print_help():
    """Print help information"""
    help_text = """
Доступные команды:
  - Введите запрос на русском языке для генерации SQL
  - 'help' или 'помощь' - показать эту справку
  - 'schema' или 'схема' - показать схему таблицы
  - 'exit' или 'выход' - выйти из программы
  
Примеры запросов:
  - "Покажи последние 10 визитов"
  - "Сколько всего записей в таблице"
  - "Покажи уникальных посетителей за последнюю неделю"
"""
    print(help_text)


def main():
    """Main application loop"""
    print_banner()
    
    # Initialize SQL generator
    try:
        generator = ClickHouseSQLGenerator()
    except ValueError as e:
        print(f"✗ Ошибка инициализации: {e}")
        print("\nПроверьте, что файл .env настроен правильно.")
        print("Скопируйте .env.example в .env и заполните все необходимые значения.")
        sys.exit(1)
    
    # Test connection
    if not generator.connect_to_clickhouse():
        print("\n⚠ Не удалось подключиться к ClickHouse.")
        print("Программа будет работать в режиме генерации SQL без выполнения запросов.")
        execute_queries = False
    else:
        execute_queries = True
    
    print_help()
    
    # Main loop
    while True:
        try:
            # Get user input
            user_input = input("\n💬 Ваш запрос: ").strip()
            
            if not user_input:
                continue
            
            # Handle special commands
            if user_input.lower() in ['exit', 'выход', 'quit', 'q']:
                print("\n👋 До свидания!")
                break
            
            if user_input.lower() in ['help', 'помощь', 'h', '?']:
                print_help()
                continue
            
            if user_input.lower() in ['schema', 'схема']:
                schema = generator.get_table_schema()
                if schema:
                    print(f"\n📋 Схема таблицы {generator.ch_table}:")
                    for col in schema:
                        print(f"  - {col['name']}: {col['type']}")
                else:
                    print("✗ Не удалось получить схему таблицы")
                continue
            
            # Generate SQL from natural language
            sql_query = generator.generate_sql(user_input)
            
            if not sql_query:
                print("✗ Не удалось сгенерировать SQL запрос")
                continue
            
            print(f"\n📝 Сгенерированный SQL:\n{sql_query}")
            
            # Ask if user wants to execute the query
            if execute_queries:
                execute = input("\n▶ Выполнить запрос? (y/n): ").strip().lower()
                
                if execute in ['y', 'yes', 'д', 'да']:
                    result = generator.execute_query(sql_query)
                    if result:
                        print("\n✓ Результаты запроса:")
                        print(generator.format_results(result))
                    else:
                        print("✗ Запрос не выполнен")
            else:
                print("\n⚠ Подключение к базе данных недоступно. Запрос не выполнен.")
        
        except KeyboardInterrupt:
            print("\n\n👋 До свидания!")
            break
        except Exception as e:
            print(f"\n✗ Произошла ошибка: {e}")


if __name__ == "__main__":
    main()
