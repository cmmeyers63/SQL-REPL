import sqlite3
import pandas as pd
import sys
import re
import pathlib
from pynput import keyboard
import os

help_str = """ 
	help will appear here
"""

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

	def debug(self, value : str):
		if not self.debug:
			print(value)

	def __init__(self, database_path : str, debug = False) -> None:
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

		while True:
			user_input: list[str] = input(self.prompt_string).split()
			match user_input:
				case ['exit'] | ['q'] | ['quit']:
					print("Goodbye...")
					self.con.close()
					exit(0)
				case ['help']:
					print(help_str)
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
					print(result_text_output)
		except Exception as e:
			print("SQL EXCEPTION:")
			print(e)
			return False

		return True

	def load_external_table(self, filepath: str):
		if not os.path.exists(filepath):
			print("specified file does not exist")
			return

		# no extension or path
		filename = pathlib.Path(filepath).stem
		match pathlib.Path(filepath).suffix:
			case '.csv':
				df : pd.DataFrame = pd.read_csv(filepath)
				
				# create the table
				sql = f"create table {filename}("
				for i, col in enumerate(df.columns):
					sql += f"'{str(col).strip()}'"
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
				self.debug(sql)
				self.con.execute(sql)

				# insert the rows
				for index, row in df.iterrows():
					sql = f"insert into {filename} values ("
					for i, cell in enumerate(row):
						match str(df.dtypes[i]):
							case "int64":
								sql += f"{cell},"
							case _:
								sql += f"'{cell}',"
					sql = sql[:len(sql)-1]
					sql += ")"
					self.debug(sql)
					self.con.execute(sql)

if __name__ == "__main__":

	debug = len(sys.argv) > 0 and sys.argv[0] == 'debug'
	repl = REPL(":memory:", debug=debug)
	repl.eval_loop()
		


