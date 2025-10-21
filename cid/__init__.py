# Declare namespace

try:
    __import__('pkgutil').extend_path(__path__, __name__)
except NameError:
    pass
