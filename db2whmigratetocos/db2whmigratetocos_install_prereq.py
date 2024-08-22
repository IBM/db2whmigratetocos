import sys
import subprocess
import os


packagers = ["yum","apt-get"]
yum_odbc = "sudo yum -y install unixODBC"
yum_pydev = "sudo yum -y  install python3-devel"
apt_odbc = "sudo apt-get -y install unixodbc"
apt_pydev = "sudo apt-get -y install python3-dev"
yum_pip = "yum -y install python-pip"
apt_pip = "sudo apt-get -y  install python3-pip"
#TODO - check for the unixODBC devel
#TODO - check for the arch other than linux



license_text = '''
LICENSE INFORMATION

The Programs listed below are licensed under the following License Information terms and conditions in addition to the Program license terms previously agreed to by Client and IBM. If Client does not have previously agreed to license terms in effect for the Program, the International License Agreement for Early Release of Programs (Z125-5544-05) applies.

Program Name (Program Number):
IBM Db2 Migration Block to COS V1.0 Tool (Early Release)

The following standard terms apply to Licensee's use of the Program.

Test Period

The test period begins on the date that Licensee agrees to the terms of this Agreement and ends on 2021-10-29.

Prohibited Uses

Licensee may not use or authorize others to use the Program if failure of the Program could lead to death, bodily injury, or property or environmental damage.

Supporting Programs

Licensee is authorized to install and use the Supporting Programs identified below only to support Licensee's use of the Principal Program under this Agreement. The phrase "to support Licensee's use" would only include those uses that are necessary or otherwise directly related to a licensed use of the Principal Program or another Supporting Program. The Supporting Programs may not be used for any other purpose. A Supporting Program may be accompanied by license terms, and those terms, if any, apply to Licensee's use of that Supporting Program. In the event of conflict, the terms in this License Information document supersede the Supporting Program's terms. Licensee must obtain sufficient entitlements to the Program, as a whole, to cover Licensee's installation and use of all of the Supporting Programs, unless separate entitlements are provided within this License Information document. For example, if this Program were licensed on a VPC (Virtual Processor Core) basis and Licensee were to install the Principal Program or a Supporting Program on a 10 VPC machine and another Supporting Program on a second 10 VPC machine, Licensee would be required to obtain 20 VPC entitlements to the Program.

Supporting Programs:
IBM db2 ODBC driver


Separately Licensed Code

Each of the components listed in the NON_IBM_LICENSE file is considered "Separately Licensed Code" licensed to Licensee under the terms of the applicable third party license agreement(s) set forth in the NON_IBM_LICENSE file(s) that accompanies the Program, and not this Agreement. Future Program updates or fixes may contain additional Separately Licensed Code. Such additional Separately Licensed Code and related licenses are listed in the applicable NON_IBM_LICENSE file that accompanies the Program update or fix. 

Note: Notwithstanding any of the terms in the third party license agreement, the Agreement, or any other agreement Licensee may have with IBM, with respect to the Separately Licensed Code: 
(a) IBM provides it to Licensee WITHOUT WARRANTIES OF ANY KIND AND DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES AND CONDITIONS INCLUDING, BUT NOT LIMITED TO, THE WARRANTY OF TITLE, NON-INFRINGEMENT OR NON-INTERFERENCE, AND THE IMPLIED WARRANTIES AND CONDITIONS OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
(b) IBM is not liable for any direct, indirect, incidental, special, exemplary, punitive or consequential damages including, but not limited to, lost data, lost savings, and lost profits.

L/N:  L-MJAI-C3ZCYF
D/N:  L-MJAI-C3ZCYF
P/N:  L-MJAI-C3ZCYF

'''

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
    print(license_text)
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
    if(sys.version_info.major > 6):
            print("python version is compatible, works with python 3.6 and above")
    else:
            print("Python version - " + str(py_version) +  " OK")
  except  Exception as e:
     print ("python is not installed. Please install python\n", e)

def setup_the_db2_driver():
    HOME = (run_command("echo $HOME")).strip()
    try:
       print(run_command("ls"))
       print(run_command("pwd"))
       find_db2_driver = run_command("find  db2whmigratetocos/db2_cli_odbc_driver/v11.5.9_linuxx64_odbc_cli.tar.gz")
       if find_db2_driver.strip() ==  "db2whmigratetocos/db2_cli_odbc_driver/v11.5.9_linuxx64_odbc_cli.tar.gz":
            print("Driver v11.5.9_linuxx64_odbc_cli.tar.gz found")
            try:
                run_command('''
                            mkdir {home}/db2_cli_odbc_driver   
                            cp  -r db2whmigratetocos/db2_cli_odbc_driver/v11.5.9_linuxx64_odbc_cli.tar.gz  {home}/db2_cli_odbc_driver
                            chown `whoami` {home}/db2_cli_odbc_driver -R
                            cd {home}/db2_cli_odbc_driver 
                            ls
                            tar xvfz v11.5.9_linuxx64_odbc_cli.tar.gz -C ./ 
                            chown `whoami` {home}/db2_cli_odbc_driver -R
                            '''.format(home=HOME))
            except Exception as e:
                print(e)
            DB2_DRIVER_SETUP=[
                "echo 'export DB2_CLI_DRIVER_INSTALL_PATH={home}/db2_cli_odbc_driver/odbc_cli/clidriver' >> ~/.bashrc ".format(home=HOME) ,
                "echo 'export LD_LIBRARY_PATH={home}/db2_cli_odbc_driver/odbc_cli/clidriver/lib' >> ~/.bashrc".format(home=HOME) ,
                "echo 'export LIBPATH={home}/db2_cli_odbc_driver/odbc_cli/clidriver/lib' >> ~/.bashrc".format(home=HOME) ,
                "echo 'export PATH={home}/db2_cli_odbc_driver/odbc_cli/clidriver/bin:$PATH' >> ~/.bashrc".format(home=HOME) ,
                "echo 'export PATH={home}/db2_cli_odbc_driver/odbc_cli/clidriver/adm:$PATH' >> ~/.bashrc".format(home=HOME) ]
            for command in DB2_DRIVER_SETUP:
                try:
                    command_executed = run_command(command)
                    print(command_executed)
                except Exception as e:
                    print(e)
       else:
          print("The driver package is not found.. aborting")    
    except Exception as e:
       print(e)
    

def check_pip_installed():
  print()
  print("checking if the pip exists ...")
  print("-------------------------------")
  try:
    py_version_output = run_command("pip3 --version")
    print ("PIP installed")
  except subprocess.CalledProcessError:
     print ("pip is not installed. Please install pip\n", subprocess.CalledProcessError)

def ODBC_driver_requirements():
    print()
    print("Checking the ODBC driver...")
    print("-------------------------------")
    try:
        odbc_driver_output = run_command("isql --version")
        print ("ODBC driver is installed")
    except subprocess.CalledProcessError:
       print ("ODBC driver is not installed. Please install ODBC driver\n", subprocess.CalledProcessError)

def check_and_Set_home_path():
   print()
   print("setting up the home path")
   try:
     HOME = run_command("echo $HOME")
     print(HOME)
   except Exception as e:
      print(e)

def unzip_the_driver():
   print()
   print("unziping the driver package")  
   try:
      find_whl = run_command("find db2whmigratetocos-0.1-py3-none-any.whl")
      if find_whl.strip() == "db2whmigratetocos-0.1-py3-none-any.whl":
        unzip_out = run_command("unzip ./db2whmigratetocos-0.1-py3-none-any.whl 'db2whmigratetocos/db2_cli_odbc_driver/*' -d .")
        print(unzip_out)
      else:
         print(".whl file not found..aborting")
   except Exception as e:
      print(e)  

def create_the_logs_folder():
    print()
    print("creating the folder to store the logs")
    try:
       HOME = run_command("echo $HOME")
       print(HOME)
       os.makedirs(HOME.strip()+"/db2whmigratetocos-logs", exist_ok = True) 
       print("Directory created in :{logs_path}".format(logs_path = HOME+"/db2whmigration-logs" )) 
    except Exception as e:
       print(e)
         
      

def db2migratetocos_env_check():
    print("----------------------------------")
    print("db2migratetocos environment check")
    print("----------------------------------")
    check_python_version()
    check_pip_installed()
    ODBC_driver_requirements()


def db2whmigratetocos_init():
    print()
    print("Welcome to IBM Db2migratetocos - An utility to move the data from block storage to COS")
    print()
    print("Read and Accept the license and the terms.")
    check_and_accept_license_terms()
    print()
    check_and_Set_home_path()
    print()
    print("Installing the needed packages")
    install_packages()
    print()
    print("Installing and setting up db2 driver")
    unzip_the_driver()
    print()
    print("Creating the logs folder")
    create_the_logs_folder()
    print()
    setup_the_db2_driver()
    print("Final Environment check for all the needed dependencies")
    db2migratetocos_env_check()
    print()
    


