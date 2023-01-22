import sqlite3
import pandas as pd
import sys
import re
import pathlib
from pynput import keyboard
from pprint import pprint
import os

help_str = """List of builtin commands:
q | quit | exit
	: quits program

load "path"
	: loads the structured data located at path into a sql table with a name corresponding to the filename
	Example: load "../data/hw_25000.csv"

ls
	: lists all tables and their creation information

Anything not matching the above commands will be treated as a SQLITE command and sent to the SQL engine for execution
	Docs: https://www.sqlite.org/lang.html"""

welcome_str = "Welcome to the SQL-REPL Environment.\nEnter 'help' for a list of all supported commands."

# flushes the terminal input screen of all text
# this allows you to write information where previous text information was displayed
def print_statusline(msg: str, override_length = 0):
	last_msg_length = 0
	if override_length == 0:
		last_msg_length = len(print_statusline.last_msg) if hasattr(print_statusline, 'last_msg') else last_msg_length
	print(' ' * last_msg_length, end='\r')
	print(msg, end='\r')
	sys.stdout.flush()
	print_statusline.last_msg = msg




# https://people.sc.fsu.edu/~jburkardt/data/csv/csv.html
class REPL():
	first_line = "$:  "
	multi_line = "... "

	def __init__(self, database_path : str) -> None:
		self.con = sqlite3.connect(":memory:")

		# prompt 
		self.prompt_string = self.first_line

		# command history
		self.command_history : list[str] = []
		self.command_index = 0		

		# keyboard event listeners
		# TODO implement command history
		# listener = keyboard.Listener(
		# 	on_release=self.key_released)
		# listener.start()

	def key_released(self, key):
		match key:
			case keyboard.Key.up:
				self.display_previous_command(1)
			case keyboard.Key.down:
				self.display_previous_command(-1)
			case _:
				pass

	def display_previous_command(self, up: int):
		if up != -1 and up != 1:
			raise RuntimeError("up must be either -1 or 1")

		# kludge to clear escape character
		print_statusline(self.prompt_string, 100)

		# bounds check before print
		if self.command_index + up < len(self.command_history) -1 \
			and self.command_index + up >= 0:
			self.command_index += up
			print_statusline(self.command_history[self.command_index])


	def eval_loop(self):
		user_input_buf : list[str] = []
		print(welcome_str)
		while True:
			user_input: list[str] = input(self.prompt_string).split()
			match user_input:
				case ['exit'] | ['q'] | ['quit']:
					print("Goodbye...")
					self.con.close()
					exit(0)
				case ['help'] | ['h']:
					print(help_str)
				case ['ls']:
					self.print_all_tables()
				# load "filepath"
				case ['load', x] if re.match(r"\".*\"", x) is not None: 
					file_path_without_quotes = x[1:len(x)-1]
					self.load_external_table(file_path_without_quotes)
				case [] if len(user_input_buf) > 0:
					self.prompt_string = self.first_line
					self.eval_sql_command(" ".join(user_input_buf))
					user_input_buf = []
				case _:
					self.prompt_string = self.multi_line
					user_input_buf.extend(user_input)


	def eval_sql_command(self, command : str) -> bool:
		try:
			print("EXEC", command)
			self.command_history.append(command)
			result = self.con.cursor().execute(command)
			if result is not None:
				result_text_output = result.fetchall()
				if result_text_output != []:
					[pprint(row) for row in result_text_output]
		except Exception as e:
			print("SQL EXCEPTION:")
			print(e)
			return False

		return True

	def print_all_tables(self):
		cursor = self.con.cursor()

		result = cursor.execute("select name from sqlite_schema WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%'").fetchall()
		if result is None or len(result) == 0:
			print("No tables loaded")
			return
		for table in result:
			# table[0] is needed because of some weird tuple packing
			sql = f"SELECT sql FROM sqlite_master WHERE name='{table[0]}';"
			print(sql)
			table = cursor.execute(sql).fetchall()
			pprint(table)


	def load_external_table(self, filepath: str):
		if not os.path.exists(filepath):
			print("fail specified file does not exist")
			return

		# no extension or path
		filename = pathlib.Path(filepath).stem
		match pathlib.Path(filepath).suffix:
			case '.csv':
				df : pd.DataFrame = pd.read_csv(filepath)
				
				# create the table
				sql = f"create table {filename}("
				for i, col in enumerate(df.columns):
					col = str(col).strip().replace("\"","")
					sql += f"'{col}'"
					match str(df.dtypes[i]):
						case "int64":
							sql += " INTEGER"
						case "float64":
							sql += " NUMERIC"
						case _:
							sql += " TEXT"
					sql += ","


				sql = sql[:len(sql)-1]
				sql += ")"
				print(sql)
				self.con.execute(sql)

				# build a template string from which we can use to insert the rows
				# ex : insert into foo values(?,?,?)
				sql_insert_template = f"insert into {filename} values("
				for _ in df.columns:
					sql_insert_template += "?,"
				sql_insert_template = sql_insert_template[:len(sql_insert_template)-1] + ")" # trim last comma

				print(sql_insert_template)
				# execute many re-uses the string and uses transaction control optimized for many sql statements
				self.con.cursor().executemany(sql_insert_template, df.values.tolist())
			case _:
				print(f"unsupported filetype {pathlib.Path(filepath).suffix}")
				return


if __name__ == "__main__":
	repl = REPL(":memory:")
	repl.eval_loop()
		


