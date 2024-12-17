
"""

    Copyright IBM Corp. 2024  All Rights Reserved.
    Licensed Materials - Property of IBM

"""

import sys
import subprocess
import os
from rich.console import Console
from .constants import PACKAGE_NAME

console = Console()

packagers = ["yum", "apt-get"]
YUM_ODBC = "sudo yum -y install unixODBC"
YUM_PYDEV = "sudo yum -y install python3-devel"
APT_ODBC = "sudo apt-get -y install unixodbc"
APT_PYDEV = "sudo apt-get -y install python3-dev"


OK = "\033[1;32;40m OK  \033[0m  \n"


LICENSE_TEXT = '''
LICENSE INFORMATION

The Programs listed below are licensed under the following License Information terms and conditions in addition to the Program license terms previously agreed to by Client and IBM. If Client does not have previously agreed to license terms in effect for the Program, the International License Agreement for Early Release of Programs (Z125-5544-05) applies.

Program Name (Program Number):
IBM Db2 Warehouse Migration Block to COS V1.0 Tool (Early Release)

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


def check_and_accept_license_terms():
    """_summary_
    """
    print()
    print("-----------------------------------------------------------------------------------------------------")
    print("Please read through the license agreement and agree to IBMs terms and conditions to proceed forward")
    print("-----------------------------------------------------------------------------------------------------")
    print(LICENSE_TEXT)
    print("-----------------------------------------------------------------------------------------------------")
    print()
    accept = input(
        "Enter one of the following options:\n 1.Accept\n 2.Decline\n")
    if int(accept) == 1:
        print("The License is accepted, proceeding with next steps....")
    else:
        print("The License is rejected, cannot continue.")
        sys.exit(1)


def run_command(command):
    """_summary_

    Args:
        command (_type_): _description_

    Returns:
        _type_: _description_
    """
    result = subprocess.check_output(command, shell=True, text=True)
    return result


def check_and_set_home_path():
    """_summary_
    """
    print()
    try:
        run_command("echo $HOME")
    except Exception as e:
        print(e)


def check_the_os_arch():
    """_summary_
    """
    print("Checking the OS")
    try:
        cmd = "arch"
        arch_found = (run_command(cmd)).strip()
        if arch_found == "x86_64":
            print("x86_64 found " + OK)
            print()
            cmd = "cat /etc/os-release | grep PRETTY_NAME"
            os_found = ((run_command(cmd)).strip()).split("=")[1]
            if "Red Hat Enterprise Linux" in os_found:
                print("Red Hat Enterprise Linux found")
            if "9.4" in os_found:
                print("Version 9.4" + OK)
        else:
            print("incompatible with the arch...aborting")
            sys.exit(0)
    except Exception as e:
        print(e)


def check_for_package_installer(packager) -> bool:
    """_summary_

    Args:
        packager (_type_): _description_

    Returns:
        bool: _description_
    """
    try:
        run_command(packager+" --version")
        return True
    except subprocess.CalledProcessError:
        print(packager+" not found")
        return False


def select_packager():
    """_summary_

    Returns:
        _type_: _description_
    """
    print()
    packager_found = False
    for packager in packagers:
        if check_for_package_installer(packager):
            packager_found = True
            return packager
    if not packager_found:
        return None


def install_packages():
    """_summary_
    """
    packager = select_packager()
    print("Packager available is: ", packager)
    if packager == "yum":
        try:
            print("Installing Python....")
            yum_pydev_output = run_command(YUM_PYDEV)
            print(yum_pydev_output)
        except Exception as e:
            print(e)
        try:
            print("Installing ODBC....")
            yum_odbc_output = run_command(YUM_ODBC)
            print(yum_odbc_output)
        except Exception as e:
            print(e)
    if packager == "apt-get":
        try:
            print("Installing Python....")
            apt_pydev_output = run_command(APT_PYDEV)
            print(apt_pydev_output)
        except Exception as e:
            print(e)
        try:
            print("Installing ODBC....")
            apt_odbc_output = run_command(APT_ODBC)
            print(apt_odbc_output)
        except Exception as e:
            print(e)
    if packager is None:
        print("NO package installer found")
        sys.exit(0)


def unzip_the_driver():
    """_summary_
    """
    print()
    print("unziping the driver package")
    try:
        find_whl = run_command("find "+PACKAGE_NAME)
        if find_whl.strip() == PACKAGE_NAME:
            unzip_out = run_command(
                "unzip ./"+PACKAGE_NAME+" 'db2whmigratetocos/db2_cli_odbc_driver/*' -d .")
            print(unzip_out)
        else:
            print(
                "Please make sure the command -db22whmigratetocos setup is run where the wheel file present")
            print(".whl file not found..aborting")
            sys.exit(0)
    except Exception as e:
        print(e)


def setup_the_db2_driver():
    """_summary_
    """
    home = (run_command("echo $HOME")).strip()
    try:
        find_db2_driver = run_command(
            "find  db2whmigratetocos/db2_cli_odbc_driver/v11.5.9_linuxx64_odbc_cli.tar.gz")
        if find_db2_driver.strip() == "db2whmigratetocos/db2_cli_odbc_driver/v11.5.9_linuxx64_odbc_cli.tar.gz":
            print("Driver v11.5.9_linuxx64_odbc_cli.tar.gz found")
            print()
            try:
                run_command('''
                            mkdir {home}/db2_cli_odbc_driver   
                            cp  -r db2whmigratetocos/db2_cli_odbc_driver/v11.5.9_linuxx64_odbc_cli.tar.gz  {home}/db2_cli_odbc_driver
                            chown `whoami` {home}/db2_cli_odbc_driver -R
                            cd {home}/db2_cli_odbc_driver 
                            ls
                            tar xvfz v11.5.9_linuxx64_odbc_cli.tar.gz -C ./ 
                            chown `whoami` {home}/db2_cli_odbc_driver -R
                            '''.format(home=home))
            except Exception as e:
                print(e)
            db2_driver_setup = [
                "echo 'export DB2_CLI_DRIVER_INSTALL_PATH={home}/db2_cli_odbc_driver/odbc_cli/clidriver' >> ~/.bashrc ".format(
                    home=home),
                "echo 'export LD_LIBRARY_PATH={home}/db2_cli_odbc_driver/odbc_cli/clidriver/lib' >> ~/.bashrc".format(
                    home=home),
                "echo 'export LIBPATH={home}/db2_cli_odbc_driver/odbc_cli/clidriver/lib' >> ~/.bashrc".format(
                    home=home),
                "echo 'export PATH={home}/db2_cli_odbc_driver/odbc_cli/clidriver/bin:$PATH' >> ~/.bashrc".format(
                    home=home),
                "echo 'export PATH={home}/db2_cli_odbc_driver/odbc_cli/clidriver/adm:$PATH' >> ~/.bashrc".format(home=home)]
            for command in db2_driver_setup:
                try:
                    run_command(command)
                except Exception as e:
                    print(e)
            print("The needed driver path variables are set")
            print()
        if "No such file or directory" in find_db2_driver:
            print("The driver package is not found.. aborting")
            sys.exit(0)
    except Exception as e:
        print("The driver package is not found.. aborting")
        sys.exit(0)


def check_pip_installed():
    """_summary_
    """

    try:
        run_command("pip3 --version")
        print("PIP installed " + OK)
    except subprocess.CalledProcessError:
        print("pip is not installed. Please install pip\n",
              subprocess.CalledProcessError)


def check_python_version():
    """_summary_
    """
    try:
        py_version_output = run_command("python3 --version")
        semantic_version = (py_version_output.split(" ")[1]).replace(".", " ")
        py_version = float(
            semantic_version[0] + '.' + semantic_version[2] + semantic_version[3])
        if sys.version_info.major > 6:
            print("python version is compatible, works with python 3.6 and above")
        else:
            print("Python version - " + str(py_version) + OK)
    except Exception as e:
        print("python is not installed. Please install python\n", e)


def odbc_driver_requirements():
    """_summary_
    """
    try:
        run_command("isql --version")
        print("ODBC driver is installed " + OK)
    except subprocess.CalledProcessError:
        print("ODBC driver is not installed. Please install ODBC driver\n",
              subprocess.CalledProcessError)


def create_the_logs_folder():
    """_summary_
    """
    try:
        home = run_command("echo $HOME")
        os.makedirs(home.strip()+"/db2whmigratetocos-logs", exist_ok=True)
        print("Directory created in :{logs_path}".format(
            logs_path=home.strip()+"/db2whmigration-logs"))
    except Exception as e:
        print(e)


def db2migratetocos_env_check():
    """_summary_
    """
    print()
    check_python_version()
    check_pip_installed()
    odbc_driver_requirements()


def db2whmigratetocos_init():
    """_summary_
    """
    print()
    console.print(
        "IBM Db2whmigratetocos - An utility to move the data from block storage to COS", style="italic cyan bold")
    print()
    check_the_os_arch()
    # print("Read and Accept the license and the terms.")
    # check_and_accept_license_terms()
    check_and_set_home_path()
    print()
    console.print("Installing the needed packages", style="italic")
    install_packages()
    print()
    console.print("Unpacking and setting up db2 driver", style="italic")
    unzip_the_driver()
    setup_the_db2_driver()
    # not needed as the logs path is provided by the user
    # console.print("Creating the logs folder", style="italic")
    # create_the_logs_folder()
    print()
    console.print(
        "Final Environment check for all the needed dependencies", style="italic")
    db2migratetocos_env_check()
    print()
