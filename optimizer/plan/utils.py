import random
import time


"""
used to generate a random id for eacn plan node
"""
random.seed(int(time.time()))
def random_id():
    return random.randint(0, 2000000000)


"""
add whitespace indents to each line of a string
"""
def add_indent(s, indent):
    return '\n'.join([(" " * indent) + line for line in s.split('\n')])
