if [ -d '.venv' ]; then
    echo "folder aready exists"
else
    echo 'python3 -m venv .venv'
    python3 -m venv .venv
fi

echo 'source .venv/bin/activate'
source .venv/bin/activate

echo 'pip3 install requests'
pip3 install requests

python3 main.py
