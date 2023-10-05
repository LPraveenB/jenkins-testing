import sys
import os

current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.dirname(current_path)
grandparent_path = os.path.dirname(parent_path)

sys.path.extend([current_path, parent_path, grandparent_path] +
                [os.path.abspath(os.path.join(current_path, dirpath)) for dirpath, _, _ in os.walk(current_path)])
