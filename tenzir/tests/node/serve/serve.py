# test : python_node

from fixture_support import *

def main():
    fixture = Executor()
    res = fixture.run("version | select version | write_lines")
    print(res.stdout)

if __name__ == "__main__" :
    main()
