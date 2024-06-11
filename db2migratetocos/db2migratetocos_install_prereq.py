import sys
import subprocess

packagers = ["yum","apt-get"]
yum_odbc = "sudo yum -y install unixODBC unixODBC-devel"
yum_pydev = "sudo yum -y  install python3-devel"
apt_odbc = "sudo apt-get -y install unixodbc unixodbc-dev"
apt_pydev = "sudo apt-get -y install python3-dev"
yum_pip = "yum -y install python-pip"
apt_pip = "sudo apt-get -y  install python3-pip"

def check_for_package_installer(packager) -> bool:
    try:
        packager_output = run_command(packager+" --version")  
        return True
    except subprocess.CalledProcessError:
        print(packager+" not found")
        return False

def select_packager():
    print("Installing the needed packages")
    print()
    print("-----------------------------------------")
    print("checking the package installer available")
    print("-----------------------------------------")
    packager_found = False
    for packager in packagers:
        if(check_for_package_installer(packager)):
          packager_found = True
          package_manager = packager
          return packager
    if not packager_found:
        print("-----------------------------------------")
        print("No package installation found")
        print("-----------------------------------------")
        return "NONE"
 
def install_packages():
    packager = select_packager()
    print("Packager avaiable is: ",packager)  
   
    if packager == "yum":
        print(run_command("yum -y update"))
        print("-----------------------------------------")
        print("Installing the packages using ", packager) 
        print("-----------------------------------------")
        try:
         print("Installing Python....") 
         yum_pydev_output = run_command(yum_pydev)
         print(yum_pydev_output)
        except Exception as e:
         print(e)
        try:
         print("Installing ODBC....") 
         print(run_command("yum -y update"))
         yum_odbc_output = run_command(yum_odbc)
         print(yum_odbc_output)
        except Exception as e:
         print(e)
    if packager == "apt-get":
        print("-----------------------------------------")
        print("Installing the packages using ", packager) 
        print("-----------------------------------------")
        try:
         print("Installing Python....") 
         apt_pydev_output = run_command(apt_pydev)
         print(apt_pydev_output)
        except Exception as e:
         print(e)
        try:
         print("Installing ODBC....") 
         apt_odbc_output = run_command(apt_odbc)
         print(apt_pydev_output)
        except Exception as e:
         print(e)
        
def check_and_accept_license_terms():
    print()
    print("-----------------------------------------------------------------------------------------------------")
    print("Please read through the license agreement and agree to IBMs terms and conditions to proceed forward")
    print("-----------------------------------------------------------------------------------------------------")
    file1 = open("./ibm_db2migratetocos_license.txt", "r+")
    print(file1.read())
    print("-----------------------------------------------------------------------------------------------------")
    print()
    try:
        accept = input("Enter one of the following options:\n 1.Accept\n 2.Decline\n")
        if int(accept) == 1:
            print("The License is accepted, proceeding with next steps....")
        else:
            print("The License is rejected, cannot continue.")
            sys.exit(0)
    except Exception as e:
        print("Unexpected literal provided. Kindly rerun the file to accept")

def run_command(command):
    print(command)
    result = subprocess.check_output(command, shell=True,text=True)
    return result
    
def check_python_version():
  print()
  print("checking the python version...")
  print("-------------------------------")
  try:
    py_version_output = run_command("python3 --version")
    print(py_version_output)
    semantic_version = (py_version_output.split(" ")[1]).replace("."," ")
    py_version = float(semantic_version[0]+ '.' + semantic_version[2] + semantic_version[3])
    print(py_version)
    if(py_version > 3.6):
            print("python version is incompatible, works with python 3.6 and above")
    else:
            print("Python version - " + str(py_version) +  " OK")
  except  Exception as e:
     print ("python is not installed. Please install python\n", e)
         
def check_pip_installed():
  print()
  print("checking if the pip exists ...")
  print("-------------------------------")
  try:
    py_version_output = run_command("pip3 --version")
    print ("PIP installed")
  except subprocess.CalledProcessError:
     print ("pip is not installed. Please install pip\n", subprocess.CalledProcessError)

def pip_install_requirements():
    print()
    print("Checking required pip packages...")
    print("-------------------------------")
    package_output = run_command("pip3 install -r ./requirements.txt")
    print(package_output)

def ODBC_driver_requirements():
    print()
    print("Checking the ODBC driver...")
    print("-------------------------------")
    try:
        odbc_driver_output = run_command("isql --version")
        print ("ODBC driver is not installed")
    except subprocess.CalledProcessError:
       print ("ODBC driver is not installed. Please install ODBC driver\n", subprocess.CalledProcessError)


def db2migratetocos_env_check():
    print("----------------------------------")
    print("db2migratetocos environment check")
    print("----------------------------------")
    check_python_version()
    check_pip_installed()
    pip_install_requirements()
    ODBC_driver_requirements()


def db2migratetocos_init():
    print()
    print("Welcome to IBM Db2migratetocos - An utility to move the data from block storage to COS")
    print()
    check_and_accept_license_terms()
    print()
    install_packages()
    print()
    db2migratetocos_env_check()

   

