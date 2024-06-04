
import subprocess
 
def check_python_version():
  print()
  print("checking the python version...")
  print("-------------------------------")
  try:
    py_version_output = run_commad("python3 --version")
    semantic_version = (py_version_output.split(" ")[1]).replace("."," ")
    py_version = float(semantic_version[0]+ '.' + semantic_version[2] + semantic_version[3])
    if(py_version > 3.6):
            print("python version is incompatible, works with python 3.6 and above")
    else:
            print("Python version - " + str(py_version) +  " OK")
  except e:
     print ("python is not installed. Please install python\n", e.output)

            
def check_pip_installed():
  print()
  print("checking if the pip exists ...")
  print("-------------------------------")
  try:
    py_version_output = run_commad("pip3 --version")
    print ("PIP installed")
  except subprocess.CalledProcessError:
     print ("pip is not installed. Please install pip\n", subprocess.CalledProcessError)


def pip_install_requirements():
    print()
    print("Checking required pip packages...")
    print("-------------------------------")
    package_output = run_commad("pip3 install -r ./requirements.txt")
    print(package_output)

def ODBC_driver_requirements():
    print()
    print("Checking the ODBC driver...")
    print("-------------------------------")
    try:
        odbc_driver_output = run_commad("isql --version")
        print ("ODBC driver is not installed")
    except subprocess.CalledProcessError:
       print ("ODBC driver is not installed. Please install ODBC driver\n", subprocess.CalledProcessError)

def run_commad(command):
    result = subprocess.check_output(command, shell=True, text=True)
    return result

check_python_version()
check_pip_installed()
pip_install_requirements()
ODBC_driver_requirements()







