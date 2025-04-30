#!/bin/bash


mkdir -p ~/dist_project
cd ~/dist_project


sudo apt-get update -y
sudo apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    git \
    wget \
    curl \



if [ ! -d "~/dist_project/.git" ]; then
    git clone https://github.com/eslam7-star/distributed_project.git .
else
    git pull origin main
fi


python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt



