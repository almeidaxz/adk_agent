import asyncio
import json
import os
import logging
import pandas as pd

import mcp.server.stdio

from pathlib import Path
from google.adk.tools.function_tool import FunctionTool
from google.adk.tools.mcp_tool.conversion_utils import adk_to_mcp_tool_type
from google.cloud.storage import Client, transfer_manager

from mcp import types as mcp_types
from mcp.server.lowlevel import NotificationOptions, Server
from mcp.server.models import InitializationOptions

from dotenv import load_dotenv

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str((Path(__file__).parent / "credentials" / "credentials.json").resolve())
LOG_FILE_PATH = os.path.join(os.path.dirname(__file__), "mcp_server_activity.log")

logging.basicConfig(
  level=logging.DEBUG,
  format="%(asctime)s - %(levelname)s -[%(filename)s:%(lineno)d] - %(message)s",
  handlers=[
    logging.FileHandler(LOG_FILE_PATH, mode="a")
  ] 
)
load_dotenv()

def get_data_from_gcs(bucket_name: str) -> str:
  """Extrai os dados do bucket designado

  Args:
    bucket_name(str): O npme do bucket de onde os dados devem ser extraídos.

  Returns:
    str: Uma mensagem de falha ou sucesso, informando os nomes dos arquivos extraídos.
  """
  logging.info("Iniciou o processo de extração do bucket")
  try:
    gcs_client = Client()
    bucket = gcs_client.bucket(bucket_name)
    temp_path = str((Path(__file__).parent / "temp").resolve())

    blob_names = [blob.name for blob in bucket.list_blobs()]

    results = transfer_manager.download_many_to_path(
        bucket, blob_names, destination_directory=temp_path, max_workers=2
    )
    logging.info(f"Objetos {results} baixados.")
    return f"Blobs {str(blob_names)} baixados em {temp_path}."
  except Exception as e:
    logging.error(f"Falha ao extrair dados do bucket: {e}")
    return f"Ocorreu um erro ao baixar os arquivos do bucket."

def check_null_columns(table_path: str) -> str:
  """Verifica a existência de campos nulos na tabela especificada e retonar os nomes das tabelas e quantidades de nulos encontrados.

  Args:
    table_path(str): O caminho completo da tabela na qual deve ser verificado a existência de campos nulos.

  Returns:
    str: Uma string com os nomes das colunas e a quantidade de nulos encontradas em cada uma delas.
  """
  logging.info("Iniciando a checagem de colunas nulas.")
  try:
    df = pd.read_csv(table_path)
    return str(df.isnull().sum())
  except Exception as e:
    logging.error(f"Falha ao analisar tabela: {e}")
    return f"Ocorreu um erro ao analisar tabela."

def drop_null_columns(table_path: str) -> str:
  """Dropa (remove) campos nulos da tabela especificada.

  Args:
    table_path(str): O caminho completo da tabela na qual devem ser dropados (removidos) os campos nulos.

  Returns:
    str: Uma string com a comparação do schema da tabela antes e depois.
  """
  logging.info(f"Iniciando a remoção das colunas nulas.")
  try:
    df = pd.read_csv(table_path)
    
    nulls_list: list = []
    for col_name in df.columns:
      if df[col_name].isnull().sum():
        nulls_list.append(col_name)
    
    schema_before = str(df.dtypes)
    df.drop(columns=nulls_list, inplace=True)
    schema_after = str(df.dtypes)

    df.to_csv(table_path)
    return "\n".join([str(schema_before), str(schema_after)])
  except Exception as e:
    logging.error(f"Falha ao tentar remover as colunas nulas: {e}")
    return f"Ocorreu um erro ao tentar remover as colunas nulas."

def get_table_schema(table_path: str) -> str:
  """Retorna o schema da tabela especificada.

  Args:
    table_path(str): O caminho completo da tabela da qual o schema dave ser retornado.

  Returns:
    str: Uma texto (str) demonstrando a estrutura e schema da tabela.
  """
  logging.info("Iniciando a leitura da tabela.")
  try:
    df = pd.read_csv(table_path)
    return str(df.dtypes)
  except Exception as e:
    logging.error(f"Falha ao ler a tabela: {e}")
    return f"Ocorreu um erro ao ler a tabela."

def fill_null(table_path: str, column_name: str, fill_value: str) -> str:
  """Preenche os campos na coluna especificada da tabela especificada como zeros.

  Args:
    table_path(str): O caminho completo da tabela na qual os campos nulos devem ser preenchidos com zero.
    column_name(str): O nome da coluna na qual os dados serão alterados.

  Returns:
    str: Uma texto (str) demonstrando 3 exemplos de como os valores estavam antes e depois, sendo o primeiro item o id da linha e o segundo a coluna em questão. 
  """
  logging.info(f"Iniciando preenchimento de campos nulos")
  try:
    df = pd.read_csv(table_path)

    id_list = df['id'].head(3).tolist()
    if not id_list:
      return f"Não foram encontradas linhas nulas na coluna '{column_name}'."

    rows_before = df[df['id'].isin(id_list)][['id', column_name]]
    df[column_name].fillna(fill_value, inplace=True)
    rows_after = df[df['id'].isin(id_list)][['id', column_name]]

    df.to_csv(table_path)
    return "\n".join([str(rows_before), str(rows_after)])
  except Exception as e:
    logging.error(f"Falha ao tentar preencher campos nulos: {e}")
    return f"Ocorreu um erro ao tentar preencher dados nulos."
    
def remove_symbols(table_path: str, column_name: str) -> str:
  """Remove caracteres não numéricos do coluna especificada da tabela.

  Args:
    table_path(str): O caminho completo da tabela na qual os campos nulos devem ser preenchidos com zero.
    column_name(str): O nome da coluna na qual os dados serão alterados.

  Returns:
    str: Uma texto (str) demonstrando 3 exemplos de como os valores estavam antes e depois, sendo o primeiro item o id da linha e o segundo a coluna em questão. 
  """
  try:
    df = pd.read_csv(table_path)
    id_list = df['id'].head(3).tolist()

    rows_before = df[df['id'].isin(id_list)][['id', column_name]]
    df[column_name] = df[column_name].str.replace(r"[^0-9.,]+", "", regex=True)
    rows_after = df[df['id'].isin(id_list)][['id', column_name]]

    df.to_csv(table_path)
    return "\n".join([str(rows_before), str(rows_after)])
  except Exception as e:
    logging.error(f"Falha ao tentar transformar dados: {e}")
    return "Ocorreu um erro ao tentar transformar os dados." 
  
app = Server("etl_server")

ADK_ETL_TOOLS = {
  "get_data_from_gcs": FunctionTool(func=get_data_from_gcs),
  "check_null_columns": FunctionTool(func=check_null_columns),
  "drop_null_columns": FunctionTool(func=drop_null_columns),
  "get_table_schema": FunctionTool(func=get_table_schema),
  "fill_null": FunctionTool(func=fill_null),
  "remove_symbols": FunctionTool(func=remove_symbols)
}

@app.list_tools()
async def list_mcp_tools() -> list[mcp_types.Tool]:
  """MCP handler que lista as tools que este server expõe."""
  logging.info("MCP Server: Iniciou a listagem de tools.")
  mcp_tools_list = []
  for tool_name, adk_tool_instance in ADK_ETL_TOOLS.items():
    if not adk_tool_instance.name:
      adk_tool_instance.name = tool_name

    mcp_tools_schema = adk_to_mcp_tool_type(adk_tool_instance)
    mcp_tools_list.append(mcp_tools_schema)
  return mcp_tools_list

@app.call_tool()
async def call_mcp_tool(name: str, arguments: dict) -> list[mcp_types.TextContent]:
  """MCP handler para executar a chamada da tool solicitada pelo MCP client."""
  logging.info(f"MCP Server: Iniciou a chamada da tool {name} com os args: {arguments}")

  if name in ADK_ETL_TOOLS:
    adk_tool_instance = ADK_ETL_TOOLS[name]
    try:
      adk_tool_response = await adk_tool_instance.run_async(
        args=arguments,
        tool_context=None
      )
      logging.info(f"MCP Server: Tool {name} executada. Resposta: {adk_tool_response}")
      response_text = json.dumps(adk_tool_response, indent=2)
      return [mcp_types.TextContent(type="text", text=response_text)]
    except Exception as e:
      logging.error(f"MCP Server: Erro ao executar tool {name}. Resposta: {e}")
      payload = {
        "success": False,
        "message": f"Falha ao executar tool '{name}': {str(e)}"
      }
      error_text = json.dumps(payload)
      return [mcp_types.TextContent(type="text", text=error_text)]
  else:
    logging.error(f"MCP Server: Erro ao executar tool. Tool não implementada.")
    payload = {
      "success": False,
      "message": f"Tool não implementada neste servidor."
    }
    error_text = json.dumps(payload)
    return [mcp_types.TextContent(type="text", text=error_text)]

async def run_mcp_stdio_server():
  """Executa o MCP server, trabalhando no stdio"""
  async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
    await app.run(
      read_stream,
      write_stream,
      InitializationOptions(
        server_name=app.name,
        server_version="0.1.0",
        capabilities=app.get_capabilities(
          notification_options=NotificationOptions(),
          experimental_capabilities={}
        )
      )
    )

if __name__ == "__main__":
  logging.info(f"Iniciando servidor MCP.")
  try:
    asyncio.run(run_mcp_stdio_server())
  except Exception as e:
    logging.critical(f"Servidor MCP encontrou um erro não tratado: {e}", exc_info=True)
  finally:
    logging.info(f"Servidor MCP encerrando.")
    logging.info(f"\n\n")
    
    