#!/bin/bash
date -u
echo "Begin setup script...."
echo "Starting dnf update"
dnf check-update
sudo -H yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm
sudo -H yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm
sudo -H yum repolist
echo  "Starting python 3.9 upgrade"
sudo -H yum install -y python3.9
echo "configuring python 3.9"
sudo -H update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1
sudo -H update-alternatives --set python3 /usr/bin/python3.9
sudo -H update-alternatives --remove python3 /usr/bin/python3.6
echo "Starting python 3.9 disutils install"
sudo -H yum install -y python3.9-distutils
echo "Downloading and installing dotnet"
sudo -H curl https://packages.microsoft.com/config/rhel/8/packages-microsoft-prod.rpm -o packages-microsoft-prod.rpm
sudo -H rpm -i packages-microsoft-prod.rpm
sudo -H yum install -y dotnet-runtime-6.0
echo "Installing Python-pip"
sudo -H yum install -y python3-pip
sudo -H mkdir /usr/simulation
sudo -H mkdir /usr/simulation/ammps_config_generator
sudo -H mkdir /usr/simulation/ammps_bin
sudo -H mkdir /usr/simulation/SHARKFin
sudo -H mkdir /usr/simulation/ammps_container
sudo -H mkdir /usr/simulation/harkrepo/
sudo chmod -R 777 /usr/simulation
echo "Installing Git"
sudo -H yum install -y git
echo "Cloning ammps_config repo"
sudo -H git clone https://github.com/mesalas/ammps_config_generator /usr/simulation/ammps_config_generator
echo "Cloning sharkfin repo"
sudo -H git clone https://github.com/sbenthall/SHARKFin /usr/simulation/SHARKFin/
echo "Cloning HARK binaries"
sudo -H git clone https://github.com/econ-ark/HARK.git /usr/simulation/harkrepo
sudo -H cp -Rf /usr/simulation/harkrepo/HARK/ /shared/home/ammpssharkfin/.local/lib/python3.9/site-packages/
echo "Cloning ammps binaries"
sudo -H git clone https://<INSERT PERSONAL ACCESS TOKEN>@github.com/mesalas/ammps_sharkfin_container.git /usr/simulation/ammps_container
sudo -H cp -Rf /usr/simulation/ammps_container/container_contents/ammps_bin/ /usr/simulation/
sudo chmod -R 777 /usr/simulation
echo "Installing Requirements using pip"
sudo -H pip3.9 install -r /shared/home/ammpssharkfin/requirements.txt 
echo "Setup complete."
date -u

