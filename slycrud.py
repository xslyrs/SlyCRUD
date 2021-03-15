#!/usr/bin/python
#encoding:utf-8

import fdb

#'/usr/share/firebird/databases/contatos.fdb'

# ----------------------------------------------------------------------------------------------------
#  SlyCRUD.py: 
#
#	Pacote python que disponibiliza uma interface de inserção, alteração, remoção e seleção de informações
#	em um banco de dados Firebird.
#
# ----------------------------------------------------------------------------------------------------	
class SlyCRUD:

# ----------------------------------------------------------------------------------------------------
# 	Construtor da classe:
#	- Objetivo: Construir a classe SlyCRUD com as informações mínimas necessárias para a utilização dos
#	  métodos do CRUD
#
#	- Parâmetros de entrada:
#		__database = nome do arquivo de banco de dados firebird a ser acessado
#		__user = usuário do banco de dados. Valor padrão = SYSDBA
#		__password = senha do banco de dados. Valor padrão = masterkey
#
#	 - Retorno do método: 
#		True or False = Verdadeiro ou falso dependendo do sucesso da conexão ao banco de dados
#
# ----------------------------------------------------------------------------------------------------	
	def __init__(self, __database, __user="SYSDBA", __password="masterkey"):
		self._database = __database
		self._user= __user
		self._password = __password
		try:
			return self._Get_Connection()
		except:
			return None
		
# ----------------------------------------------------------------------------------------------------
# 	Método GetConnetion:
#	 - Objetivo: Conectar a base de dados definida na construção da classe
#
#	 - Parâmetros de entrada: Nenhum
#
#	 - Retorno do método: 
#		True or False = Verdadeiro ou falso dependendo do sucesso da conexão ao banco de dados
#
# ----------------------------------------------------------------------------------------------------
	def _Get_Connection(self):
		try:
			self._connection = fdb.connect(
				dsn='localhost:''+self._database,
				user=self._user,
				password=self._password
			)
			print(' > Connection Sucessfull')
			self._cursor = self._connection.cursor()
			return True
		except Exception as e:
			if hasattr(e, 'message'):
				print(' > ',e.message)
			else:
				print(' > ',e)
			return False
		
# ----------------------------------------------------------------------------------------------------
#	Método _Get_AllTableData: 
#	 - Objetivo: Trazer toda a informação de uma tabela do banco de dados
#
#	 - Parâmetros de entrada:
#		__tablename = tabela a ser buscada
#		__orderby = coluna a ser definida como chave de ordenação. Valor padrão = id
#
#	 - Retorno do método:
#		True or False = Verdadeiro ou falso dependendo do sucesso da busca
#
# ----------------------------------------------------------------------------------------------------	
	def _Get_AllTableData(self, __tablename, __orderby="id"):
		try:
			sql = """ select * from {} order by {}""".format(__tablename, __orderby)
			self.cursor.execute(sql)
			self.connection.commit()
			return True
		except Exception as e:
			if hasattr(e, 'message'):
				print(' > ',e.message)
			else:
				print(' > ',e)
			return False

# ----------------------------------------------------------------------------------------------------
#	Método _Get_TableDataUponConditions: 
#	 - Objetivo: Trazer toda a informação de uma tabela do banco de dados, baseada em certas condições de busca
#
#	 - Parâmetros de entrada:
#		__tablename = tabela a ser buscada
#		__conditions = condição imposta para a realização da busca na tabela em questão
#		__orderby = coluna a ser definida como chave de ordenação. Valor padrão = id
#
#	 - Retorno do método:
#		True or False = Verdadeiro ou falso dependendo do sucesso da busca
#
# ----------------------------------------------------------------------------------------------------	
	def _Get_TableDataUponConditions(self, __tablename, __conditions="", __orderby="id"):
		if __conditions=="":
			return self._Get_AllTableData(__table)
		else:
			try:
				sql = """ select * from {} where {} order by {}""".format(__tablename, __conditions, __orderby)
				self.cursor.execute(sql)
				self.connection.commit()
				return True
			except Exception as e:
				if hasattr(e, 'message'):
					print(' > ',e.message)
				else:
					print(' > ',e)
				return False
				
# ----------------------------------------------------------------------------------------------------
#	Método _Set_DataUponConditions: 
#	 - Objetivo: realizar uma alteração de dado em uma tabela, baseado em certas condições
#
#	 - Parâmetros de entrada:
#		__tablename = tabela a ser alterado o valor
#		__columns_and_values = lista de colunas da tabela que receberá a alteração. Formato esperado = [ [coluna1,valor1], [coluna2,valor2], ... ]
#		__conditions = condição imposta para a realização da alteração da informação. Formato esperado = " coluna1=valor1 and coluna2=valor2 or coluna3=valor3"
#
#	 - Retorno do método:
#		True or False = Verdadeiro ou falso dependendo do sucesso da operação
#
# ----------------------------------------------------------------------------------------------------	
	def _Set_DataUponConditions(self, __tablename, __columns_and_values=[], __condition=""):
		try:
			if __columns_and_values.length == 0:
				raise Exception("It is impossible to make any updates without a column and value defined")
			_columns_and_values=""
			for item in __columns_and_values:
				_columns_and_values += item[0]+'='item[1]+' '
			sql = """ update {} set {} where {}""".format(__tablename,_columns_and_values,__condition)
			self.cursor.execute(sql)
			self.connection.commit()
			return True
		except Exception as e:
			if hasattr(e, 'message'):
				print(' > ',e.message)
			else:
				print(' > ',e)
			return False
			
# ----------------------------------------------------------------------------------------------------
# 	Método _ValidateAndFormat_ColumnArray
#	 - Objetivo:
#		Validar e construir as strings para as operações de Create, Insert e Update do CRUD
#
#	 - Parâmetros de entrada:
#		__operation = parâmetro do tipo choice para um dos valores 'create', 'insert' ou 'update'
#		__arr = lista de valores a ser trabalhado para a criação da string. Formato esperado:
#			'create' = [ ['nomecoluna1', 'tipo e demais opções da coluna1'], ['nomecoluna2', 'tipo e demais opções da coluna2'], ... ]
#			'insert' = [ ['nomecoluna1','valorcoluna1'], ['nomecoluna2','valorcoluna2'], ['nomecoluna3','valorcoluna3'], ... ]
#			'update' = [ ['nomecoluna1','valorcoluna1'], ['nomecoluna2','valorcoluna2'], ['nomecoluna3','valorcoluna3'], ... ]
#
#	 - Retorno do método:
#		O retorno irá depender do tipo de operação definido. Caso o parâmetro __operation seja:
#			'create' o retorno é igual uma string formatada com o campo e o tipo separado por vírgulas
#			'insert' o retorno é igual uma lista de 2 ítens de variaveis string formatada por vírgulas, onde o 1 item da string1 equivale o valor do 1 item da string2
#			'update' o retorno é igual uma string com a coluna1=valor1 separado por vírgulas 
#
# ----------------------------------------------------------------------------------------------------	
	def _ValidateAndFormat_ColumnArray(self, __operation=('create','insert','update'), __arr):
		try:
			for item in __columns:
				if __operation == 'create':
					if item[0] == 'id':
						raise Exception("The 'id' column do not must be explicit on columns array.")
					elif item[0] == 'created_at':
						raise Exception("The 'created_at' column do not must be explicit on columns array.")
				
				if item[1] == "" or item[1]==None:
					if __operation == 'create':
						raise Exception("The datatype of column '{}' must be explicit on columns array.".format(item[0]))
					else:
						raise Exception("The value of column '{}' must be explicit on columns array.".format(item[0]))
				else:
					if __operation == 'create':
						_column_schema += item[0]+' 'item[1]+','
					elif __operation == 'insert':
						_formmated_column += item[0]+','
						_formmated_values += item[1]+','
					else:
						_column_schema += item[0]+'='+item[1]+','
			
			if __operation == 'create':
				return _column_schema
			elif __operation == 'insert':
				return [_formmated_column[:-1],_formmated_values[:-1]]
			else:
				return _column_schema[:-1]
				
		except Exception as e:
			if hasattr(e, 'message'):
				print(' > ',e.message)
			else:
				print(' > ',e)
			return None

# ----------------------------------------------------------------------------------------------------
# 	Método _Add_Data
#	 - Objetivo:
#		Inserir informações em uma tabela do banco de dados
#
#	 - Parâmetros de entrada:
#		__tablename = tabela a ser inserido o dado
#		__column_and_values = colunas e valores a serem inseridos. Formato esperado = [ [coluna1,valor1], [coluna2,valor2], ... ]
#
#	 - Retorno do método:
#		True or False = Verdadeiro ou falso dependendo do sucesso da operação
#
# ----------------------------------------------------------------------------------------------------	
	def _Add_Data(self, __tablename, __column_and_values):
		[ _formmated_column, formmated_values ] = _ValidateAndFormat_ColumnArray(__operation='insert', __column_and_values)
		try:
			sql = """ insert into {} ({}) values ({}) """.format(__tablename,_formmated_column, formmated_values)
			self.cursor.execute(sql)
			self.connection.commit()
			return True
		except Exception as e:
			if hasattr(e, 'message'):
				print(' > ',e.message)
			else:
				print(' > ',e)
			return False
			
			
# ----------------------------------------------------------------------------------------------------
# 	Método _CreateTable
#	 - Objetivo:
#		Criar uma tabela no banco de dados
#
#	 - Parâmetos de entrada:
#		__tablename = nome da tabela a ser criada
#		__columns = colunas que serão inseridas na tabela. Formato esperado = [ ['nomecoluna1', 'tipo e demais opções da coluna1'], ['nomecoluna2', 'tipo e demais opções da coluna2'], ... ]
#
#	 - Retorno do método:
#		True or False = Verdadeiro ou falso dependendo do sucesso da operação
#
# ----------------------------------------------------------------------------------------------------
	def _CreateTable(self, __tablename, __columns = []):	
		try:	
			_column_schema = _ValidateAndFormat_ColumnArray(__operation='create', __columns)
			if _column_schema != None:
				sql = """ create table {} (
					id int primary key not null,
					{}
					created_at timestamp default current_timestamp);""".format(__tablename,_column_schema)
				self.cursor.execute(sql)
				self.connection.commit()
				return True
		except Exception as e:
			if hasattr(e, 'message'):
				print(' > ',e.message)
			else:
				print(' > ',e)
			return False
				
# ----------------------------------------------------------------------------------------------------
# 	 Método _Print_AllCursorData
#	 - Objetivo:
#		Imprimir no console o conteúdo do cursor da conexão do banco de dados firebird
#
#	 - Parâmetros de entrada: Nenhum
#
#	 - Retorno do método:
#		True or False = Verdadeiro ou falso dependendo do sucesso da operação
#
# ----------------------------------------------------------------------------------------------------		
	def _Print_AllCursorData(self):
		try:
			for row in self.cursor.fetchall():
				print(row)
			return True
		except Exception as e:
			if hasattr(e, 'message'):
				print(' > ',e.message)
			else:
				print(' > ',e)
			return False
	

