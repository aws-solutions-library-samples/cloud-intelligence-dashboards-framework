import sys
import subprocess


current_branch = subprocess.check_output('git branch --show-current', shell=True).decode('utf-8').strip()
subprocess.check_output('git checkout main', shell=True)
subprocess.check_output('git pull', shell=True)

from cid._version import __version__ as old_ver

bump = 'patch'
if 'minor' in sys.argv:
	bump = 'minor'

maj, minor, patch = map(int, old_ver.split('.'))

if bump=='patch':
	new_ver = '.'.join(map(str,[maj, minor, patch + 1]))
elif bump=='minor':
	new_ver = '.'.join(map(str,[maj, minor + 1, 0]))
else:
	raise NotImplementedError('only patch and minor are implemented')

print(subprocess.check_output(f"git checkout -b 'release/{new_ver}'", shell=True).decode('utf-8').strip())

tx = open('cid/_version.py').read()
with open('cid/_version.py', "w") as f:
	f.write(tx.replace(f"version__ = '{old_ver}'", f"version__ = '{new_ver}'"))

tx = open('cfn-templates/cid-cfn.yml').read()
with open('cfn-templates/cid-cfn.yml', "w") as f:
	f.write(tx.replace(f"{old_ver}", f"{new_ver}"))


print(subprocess.check_output('git diff HEAD --unified=0 --color', shell=True).decode('utf-8'))

if '--merge' in sys.argv:
	print(subprocess.check_output(f"git commit -am 'release {new_ver}'", shell=True).decode('utf-8').strip())
	print(subprocess.check_output(f"git checkout {current_branch}", shell=True).decode('utf-8').strip())
	print(subprocess.check_output(f"git merge 'release/{new_ver}'", shell=True).decode('utf-8').strip())
	print(subprocess.check_output(f"git branch -D 'release/{new_ver}'", shell=True).decode('utf-8').strip())
	print(f'Merged to {current_branch}')
else:
	print('to undo:\n git checkout HEAD -- cfn-templates/cid-cfn.yml cid/_version.py')
	print(f"to continue:\n git commit -am 'release {new_ver}'; git push origin 'release/{new_ver}'")
