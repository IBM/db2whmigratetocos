#!/bin/sh
echo "Checking the Python and the PIP version"
#checking python and pip
py_version=$(python3 --version)
if [[ "$py_version" =~ "Python" ]]; then
   echo "$py_version"
fi
pip_exits=$(pip3 --version)
if [[ "$pip_exits" =~ "pip" ]]; then
  echo "PIP exists"
  echo "$pip_exits"
fi
echo "PIP install dependencies"
# untar and install pip dependencies

pipdeps=$(ls | grep -G pip-files.tar.gz)
tar -xvzf "$pipdeps"
pipfiles=$(ls | grep -G pip-files)
pip3 install pip-files/* --force-reinstall

echo "Db2whmigratetocos tool"
db2whmigratetocoswheel=$( ls |  grep -G  *.whl)
echo "$db2whmigratetocoswheel"
pip3 install "$db2whmigratetocoswheel" --force-reinstall
is_found=0
echo "Odbc installation checking"
odbccheck=$(odbcinst -j)
if [[ "$odbccheck" =~ "unixODBC" ]]; then
    echo "${odbccheck}" | head -1
    echo "Checking for Db2 Driver DSN"
    cat /etc/odbcinst.ini > odbc.txt
    arr1=()
    while IFS= read -r line; do
        arr1+=("$line")
    done < odbc.txt
    DB2DRIVER=0
    for a in "${!arr1[@]}"; do
        if [[ "${arr1[a]}" =~ "/opt/ibm/db2/" ]]; then #inclucde the IBM case as wellm 
          echo "${arr1[a]}"
          DB2DRIVER=$a
          is_found=1
        fi
    done
    if [[ "$is_found" == 1 ]]; 
    then
      regex="\[[^\]]*\]"
      echo "The available list of DSN for Db2"
      for (( c=$DB2DRIVER; c>=0; c-- )); 
      do
      if [[ "${arr1[$c]}" =~ '['  ]]; then
          echo "${arr1[$c]}"
          break
      fi    
      done
      echo "Change into the DB2 instance user, start using the tool with the Db2 Driver DSN"
    fi
else
      echo "Db2 Driver not found. Run "db2whmigratetocos setup" to install obdc driver and set up Db2 driver"
fi



 

