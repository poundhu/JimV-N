#!/usr/bin/env bash

export PYPI='https://mirrors.aliyun.com/pypi/simple/'
export JIMVN_DOWNLOAD_URL='https://github.com/jamesiter/JimV-N/archive/master.tar.gz'

sed -i 's@"daemon": true@"daemon": false@' /etc/jimvn.conf

mkdir -p /usr/local/JimV-N
curl -sL ${JIMVN_DOWNLOAD_URL} | tar -zxf - --strip-components 1 -C /usr/local/JimV-N

mkdir -p ~/.pip
cat > ~/.pip/pip.conf << EOF
[global]
index-url = ${PYPI}
EOF

# 创建 python 虚拟环境
pip install virtualenv
virtualenv --system-site-packages /usr/local/venv-jimv

# 导入 python 虚拟环境
source /usr/local/venv-jimv/bin/activate

# 自动导入 python 虚拟环境
echo '. /usr/local/venv-jimv/bin/activate' >> ~/.bashrc

# 安装 JimV-N 所需扩展库
grep -v "^#" /usr/local/JimV-N/requirements.txt | xargs -n 1 pip install -i ${PYPI}

/usr/bin/cp -v /usr/local/JimV-N/misc/jimvn.service /etc/systemd/system/jimvn.service
systemctl daemon-reload

systemctl start jimvn.service
systemctl enable jimvn.service

systemctl status jimvc.service -l

