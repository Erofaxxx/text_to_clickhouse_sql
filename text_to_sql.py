#!/usr/bin/env python3
"""
Text to ClickHouse SQL Converter
Конвертер текстовых запросов на русском языке в SQL запросы для ClickHouse
Использует встроенную функцию AI SQL generation в ClickHouse
"""

import os
import sys
import subprocess
import tempfile
import re
import ssl
import base64
import urllib.request
import urllib.parse
import urllib.error
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class ClickHouseSQLGenerator:
    """Main class for converting natural language to ClickHouse SQL using ClickHouse built-in AI"""
    
    def __init__(self):
        """Initialize the SQL generator with ClickHouse connection and AI configuration"""
        # ClickHouse configuration
        self.ch_host = os.getenv('CLICKHOUSE_HOST', '').replace('https://', '').replace('http://', '')
        self.ch_port = int(os.getenv('CLICKHOUSE_PORT', '8443'))
        self.ch_user = os.getenv('CLICKHOUSE_USER')
        self.ch_password = os.getenv('CLICKHOUSE_PASSWORD')
        self.ch_database = os.getenv('CLICKHOUSE_DATABASE')
        self.ch_table = os.getenv('CLICKHOUSE_TABLE', 'visits_complete')
        self.ch_ssl_cert = os.getenv('CLICKHOUSE_SSL_CERT_PATH')
        
        # AI configuration - supports multiple API keys
        # Priority: OPENROUTER_API_KEY > ANTHROPIC_API_KEY > OPENAI_API_KEY
        self.openrouter_key = os.getenv('OPENROUTER_API_KEY')
        self.anthropic_key = os.getenv('ANTHROPIC_API_KEY')
        self.openai_key = os.getenv('OPENAI_API_KEY')
        
        # Determine which AI service to use
        if self.openrouter_key:
            self.ai_provider = 'openrouter'
            self.ai_api_key = self.openrouter_key
        elif self.anthropic_key:
            self.ai_provider = 'anthropic'
            self.ai_api_key = self.anthropic_key
        elif self.openai_key:
            self.ai_provider = 'openai'
            self.ai_api_key = self.openai_key
        else:
            raise ValueError("No AI API key found. Set OPENROUTER_API_KEY, ANTHROPIC_API_KEY, or OPENAI_API_KEY")
        
        # Create config file for ClickHouse AI settings
        self.config_file = None
        self.create_clickhouse_config()
        
        # Determine if using HTTP interface (ports 8443, 443, 8123)
        self.use_http = self.ch_port in (8443, 443, 8123)
        
        # Connection test status
        self.connection_ok = False
    
    def _build_http_url(self):
        """Build the base URL for ClickHouse HTTP interface"""
        scheme = 'http' if self.ch_port == 8123 else 'https'
        return f"{scheme}://{self.ch_host}:{self.ch_port}/"
    
    def _execute_http_query(self, query, timeout=10):
        """Execute a query via ClickHouse HTTP interface
        
        Args:
            query (str): SQL query to execute
            timeout (int): Request timeout in seconds
            
        Returns:
            tuple: (success, output, error) where success is bool
        """
        url = self._build_http_url()
        
        # Build query parameters (database only, credentials via Basic Auth)
        params = {}
        if self.ch_database:
            params['database'] = self.ch_database
        
        if params:
            url = url + '?' + urllib.parse.urlencode(params)
        
        # Configure SSL
        ssl_context = None
        if self.ch_port != 8123:
            ssl_context = ssl.create_default_context()
            if self.ch_ssl_cert and os.path.exists(self.ch_ssl_cert):
                ssl_context.load_verify_locations(os.path.abspath(self.ch_ssl_cert))
        
        try:
            req = urllib.request.Request(
                url,
                data=query.encode('utf-8'),
                method='POST'
            )
            
            # Use HTTP Basic Auth for credentials
            if self.ch_user and self.ch_password:
                credentials = base64.b64encode(
                    f"{self.ch_user}:{self.ch_password}".encode('utf-8')
                ).decode('ascii')
                req.add_header('Authorization', f'Basic {credentials}')
            
            response = urllib.request.urlopen(req, timeout=timeout, context=ssl_context)
            output = response.read().decode('utf-8')
            return True, output, None
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8', errors='replace')
            return False, None, error_body
        except urllib.error.URLError as e:
            return False, None, str(e.reason)
        except Exception as e:
            return False, None, str(e)
    
    def create_clickhouse_config(self):
        """Create ClickHouse client configuration file with AI settings"""
        config_content = ""
        
        if self.ai_provider == 'openrouter':
            # OpenRouter configuration (uses OpenAI-compatible API)
            config_content = f"""ai:
  provider: openai
  api_key: {self.ai_api_key}
  base_url: https://openrouter.ai/api/v1
  model: anthropic/claude-3.5-sonnet
  temperature: 0.0
  max_tokens: 1000
  timeout_seconds: 30
  enable_schema_access: true
"""
        elif self.ai_provider == 'anthropic':
            config_content = f"""ai:
  provider: anthropic
  api_key: {self.ai_api_key}
  model: claude-3-5-sonnet-20241022
  temperature: 0.0
  max_tokens: 1000
  timeout_seconds: 30
  enable_schema_access: true
"""
        elif self.ai_provider == 'openai':
            config_content = f"""ai:
  provider: openai
  api_key: {self.ai_api_key}
  model: gpt-4o
  temperature: 0.0
  max_tokens: 1000
  timeout_seconds: 30
  enable_schema_access: true
"""
        
        # Add SSL CA certificate configuration if specified
        if self.ch_ssl_cert and os.path.exists(self.ch_ssl_cert):
            ca_path = os.path.abspath(self.ch_ssl_cert)
            config_content += f"""openSSL:
  client:
    caConfig: {ca_path}
"""

        # Create temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_content)
            self.config_file = f.name
    
    def __del__(self):
        """Cleanup temporary config file"""
        if self.config_file and os.path.exists(self.config_file):
            try:
                os.unlink(self.config_file)
            except OSError:
                pass
    
    def connect_to_clickhouse(self):
        """Test connection to ClickHouse database"""
        try:
            if self.use_http:
                success, output, error = self._execute_http_query("SELECT 1")
                if success:
                    print("✓ Успешно подключено к ClickHouse базе данных")
                    self.connection_ok = True
                    return True
                else:
                    print("✗ Ошибка подключения к ClickHouse")
                    print("  Проверьте настройки подключения в файле .env")
                    if error:
                        error_line = error.split('\n')[0]
                        print(f"  {error_line}")
                    return False
            else:
                cmd = self._build_clickhouse_command("SELECT 1")
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                
                if result.returncode == 0:
                    print("✓ Успешно подключено к ClickHouse базе данных")
                    self.connection_ok = True
                    return True
                else:
                    print("✗ Ошибка подключения к ClickHouse")
                    print("  Проверьте настройки подключения в файле .env")
                    if result.stderr:
                        error_line = result.stderr.split('\n')[0]
                        print(f"  {error_line}")
                    return False
        except subprocess.TimeoutExpired:
            print("✗ Превышено время ожидания подключения к ClickHouse")
            return False
        except Exception as e:
            print("✗ Ошибка подключения к ClickHouse")
            print("  Проверьте настройки подключения в файле .env")
            return False
    
    def _build_clickhouse_command(self, query, extra_args=None):
        """Build clickhouse-client command with all necessary parameters
        
        Args:
            query (str): SQL query to execute
            extra_args (list): Additional command line arguments
            
        Returns:
            list: Command and arguments for subprocess
        """
        cmd = ['clickhouse-client']
        
        # Connection parameters
        if self.ch_host:
            cmd.extend(['--host', self.ch_host])
        
        # When HTTP port is configured, use native secure port for clickhouse-client
        if self.use_http:
            native_port = int(os.getenv('CLICKHOUSE_NATIVE_PORT', '9440'))
            cmd.extend(['--port', str(native_port)])
        elif self.ch_port:
            cmd.extend(['--port', str(self.ch_port)])
        if self.ch_user:
            cmd.extend(['--user', self.ch_user])
        if self.ch_password:
            cmd.extend(['--password', self.ch_password])
        if self.ch_database:
            cmd.extend(['--database', self.ch_database])
        
        # SSL settings
        cmd.append('--secure')
        
        # AI configuration file
        if self.config_file:
            cmd.extend(['--config-file', self.config_file])
        
        # Add extra arguments if provided
        if extra_args:
            cmd.extend(extra_args)
        
        # Query
        cmd.extend(['--query', query])
        
        # Output format (default)
        if not extra_args or '--format' not in extra_args:
            cmd.extend(['--format', 'TabSeparated'])
        
        return cmd
    
    def get_table_schema(self):
        """Get table schema information from ClickHouse"""
        if not self.connection_ok:
            if not self.connect_to_clickhouse():
                return None
        
        try:
            query = f"DESCRIBE TABLE {self.ch_database}.{self.ch_table}"
            
            if self.use_http:
                success, output, error = self._execute_http_query(query)
                if success and output:
                    schema = []
                    for line in output.strip().split('\n'):
                        if line:
                            parts = line.split('\t')
                            if len(parts) >= 2:
                                schema.append({
                                    'name': parts[0],
                                    'type': parts[1],
                                })
                    return schema
                else:
                    print("✗ Ошибка при получении схемы таблицы")
                    return None
            else:
                cmd = self._build_clickhouse_command(query)
                
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                
                if result.returncode == 0 and result.stdout:
                    schema = []
                    for line in result.stdout.strip().split('\n'):
                        if line:
                            parts = line.split('\t')
                            if len(parts) >= 2:
                                schema.append({
                                    'name': parts[0],
                                    'type': parts[1],
                                })
                    return schema
                else:
                    print("✗ Ошибка при получении схемы таблицы")
                    return None
        except Exception as e:
            print("✗ Ошибка при получении схемы таблицы")
            return None
    
    def generate_sql(self, natural_query):
        """
        Generate SQL query from natural language using ClickHouse built-in AI
        
        Args:
            natural_query (str): Natural language query in Russian
            
        Returns:
            str: Generated SQL query or None if failed
        """
        if not self.connection_ok:
            if not self.connect_to_clickhouse():
                return None
        
        try:
            print("\n⏳ Генерация SQL запроса с использованием ClickHouse AI...")
            
            # Use ClickHouse's built-in AI SQL generation with ?? prefix
            ai_query = f"?? {natural_query}"
            
            # Build command using helper method with multiline mode
            cmd = self._build_clickhouse_command(ai_query, extra_args=['--multiline'])
            
            # Run the command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60  # AI generation may take longer
            )
            
            if result.returncode == 0:
                # Extract SQL from output
                output = result.stdout.strip()

                if not output:
                    print("✗ Получен пустой ответ от ClickHouse AI")
                    if result.stderr:
                        print(f"  Stderr: {result.stderr.strip()}")
                    return None

                # The output should contain the generated SQL
                # Parse it to extract just the SQL query
                sql_query = self._extract_sql_from_output(output)

                if sql_query:
                    return sql_query
                else:
                    # If we can't parse, show what we got and return None
                    print("⚠ Не удалось извлечь SQL из ответа AI")
                    print("  Полный вывод:")
                    # Show first few lines of output for debugging
                    output_lines = output.split('\n')
                    for i, line in enumerate(output_lines[:10]):  # Show first 10 lines
                        print(f"    {line}")
                    if len(output_lines) > 10:
                        print(f"    ... ({len(output_lines) - 10} больше строк)")
                    return None
            else:
                print("✗ Ошибка при генерации SQL запроса")
                if result.stderr:
                    # Check for specific error messages
                    if 'AI features' in result.stderr or 'API key' in result.stderr:
                        print("  Проверьте настройки AI API ключа")
                    elif 'Connection refused' in result.stderr or 'connect' in result.stderr.lower():
                        print("  Не удалось подключиться к ClickHouse")
                        print("  Проверьте настройки CLICKHOUSE_HOST, CLICKHOUSE_PORT (нативный порт)")
                    else:
                        # Show first line of error
                        error_lines = result.stderr.strip().split('\n')
                        print(f"  {error_lines[0]}")
                        if len(error_lines) > 1:
                            print(f"  (и ещё {len(error_lines) - 1} строк ошибки)")
                return None
                
        except subprocess.TimeoutExpired:
            print("✗ Превышено время ожидания ответа от AI")
            print("  Попробуйте упростить запрос или повторите позже")
            return None
        except Exception as e:
            print(f"✗ Ошибка при генерации SQL запроса")
            return None
    
    def _extract_sql_from_output(self, output):
        """Extract SQL query from ClickHouse AI output"""
        # The AI output typically contains the SQL query
        # We need to extract it, removing any explanatory text

        if not output or not output.strip():
            return None

        lines = output.split('\n')
        sql_lines = []
        in_sql = False
        skip_commentary = True

        for line in lines:
            # Skip empty lines at the beginning
            if not line.strip() and not sql_lines:
                continue

            # Skip lines that look like AI commentary or progress indicators
            if skip_commentary and (
                line.startswith('Starting AI') or
                line.startswith('──') or
                line.startswith('🔍') or
                line.startswith('✨') or
                line.startswith('➜') or
                'generated successfully' in line.lower() or
                'list_databases' in line.lower() or
                'list_tables' in line.lower() or
                'get_schema' in line.lower()
            ):
                continue

            # Look for SQL keywords to identify SQL content
            if re.match(r'^\s*(SELECT|INSERT|UPDATE|DELETE|WITH|CREATE|ALTER|DROP|SHOW|DESCRIBE|EXPLAIN)',
                       line, re.IGNORECASE):
                in_sql = True
                skip_commentary = False

            if in_sql:
                # Once we start collecting SQL, keep all lines (including empty ones)
                sql_lines.append(line)

        if sql_lines:
            # Remove trailing empty lines
            while sql_lines and not sql_lines[-1].strip():
                sql_lines.pop()
            if sql_lines:
                return '\n'.join(sql_lines).strip()

        # If we couldn't find SQL keywords, check if the output looks like SQL
        # by checking for common SQL patterns
        output_clean = output.strip()
        if re.search(r'\b(SELECT|FROM|WHERE|GROUP BY|ORDER BY|LIMIT|DESCRIBE|SHOW)\b',
                     output_clean, re.IGNORECASE):
            # Remove the commentary section if present
            if '──────────────────────────────────────────────────' in output_clean:
                parts = output_clean.split('──────────────────────────────────────────────────')
                if len(parts) > 1:
                    # Take the last part (after the last separator)
                    potential_sql = parts[-1].strip()
                    if potential_sql:
                        return potential_sql

            # Otherwise return the full output
            return output_clean

        # No SQL found
        return None
    
    def execute_query(self, sql_query, limit=10):
        """
        Execute SQL query on ClickHouse
        
        Args:
            sql_query (str): SQL query to execute
            limit (int): Maximum number of rows to return
            
        Returns:
            tuple: (success, output) where success is bool and output is string
        """
        if not self.connection_ok:
            if not self.connect_to_clickhouse():
                return False, None
        
        try:
            # Add LIMIT if not present and it's a SELECT query
            if 'LIMIT' not in sql_query.upper() and sql_query.strip().upper().startswith('SELECT'):
                sql_query = f"{sql_query} LIMIT {limit}"
            
            if self.use_http:
                success, output, error = self._execute_http_query(sql_query, timeout=30)
                if success:
                    return True, output.strip() if output else None
                else:
                    print("✗ Ошибка при выполнении запроса")
                    if error:
                        error_line = error.split('\n')[0]
                        print(f"  {error_line}")
                    return False, None
            else:
                cmd = self._build_clickhouse_command(sql_query)
                
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if result.returncode == 0:
                    return True, result.stdout.strip()
                else:
                    print("✗ Ошибка при выполнении запроса")
                    if result.stderr:
                        error = result.stderr.split('\n')[0]
                        print(f"  {error}")
                    return False, None
        except subprocess.TimeoutExpired:
            print("✗ Превышено время ожидания выполнения запроса")
            return False, None
        except Exception as e:
            print("✗ Ошибка при выполнении запроса")
            return False, None
    
    def format_results(self, output):
        """
        Format query results for display
        
        Args:
            output (str): Raw output from clickhouse-client
            
        Returns:
            str: Formatted results
        """
        if not output:
            return "Нет результатов"
        
        try:
            lines = output.strip().split('\n')
            
            if not lines:
                return "Запрос выполнен успешно. Результатов: 0"
            
            # For TabSeparated format, we need to format it as a table
            # Split each line by tabs
            rows = [line.split('\t') for line in lines]
            
            if not rows:
                return "Нет результатов"
            
            # Calculate column widths
            num_cols = len(rows[0])
            col_widths = [0] * num_cols
            
            for row in rows:
                for i, val in enumerate(row):
                    if i < num_cols:
                        col_widths[i] = max(col_widths[i], len(str(val)))
            
            # Format rows
            formatted_rows = []
            for row in rows:
                formatted_row = " | ".join([
                    str(val).ljust(col_widths[i]) 
                    for i, val in enumerate(row) if i < num_cols
                ])
                formatted_rows.append(formatted_row)
            
            # Create separator
            separator = "-+-".join(["-" * w for w in col_widths])
            
            # For first row as header
            if len(formatted_rows) > 1:
                result = [
                    "",
                    formatted_rows[0],
                    separator,
                    *formatted_rows[1:],
                    "",
                    f"Всего строк: {len(rows) - 1}"
                ]
            else:
                result = [
                    "",
                    *formatted_rows,
                    "",
                    f"Всего строк: {len(rows)}"
                ]
            
            return "\n".join(result)
        except Exception as e:
            # If formatting fails, return raw output
            return f"\n{output}\n"


def print_banner():
    """Print application banner"""
    banner = """
╔══════════════════════════════════════════════════════════════╗
║   Text to ClickHouse SQL - AI-Powered Query Generator       ║
║   Конвертер текстовых запросов в SQL для ClickHouse         ║
║   Использует встроенную функцию ClickHouse AI generation    ║
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
        print(f"✓ AI Provider: {generator.ai_provider}")
    except ValueError as e:
        print(f"✗ Ошибка инициализации: {e}")
        print("\nПроверьте, что файл .env настроен правильно.")
        print("Скопируйте .env.example в .env и заполните все необходимые значения.")
        print("Требуется один из ключей: OPENROUTER_API_KEY, ANTHROPIC_API_KEY, OPENAI_API_KEY")
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
                    success, output = generator.execute_query(sql_query)
                    if success and output:
                        print("\n✓ Результаты запроса:")
                        print(generator.format_results(output))
                    elif success:
                        print("\n✓ Запрос выполнен успешно")
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
