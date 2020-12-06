

## Setup normally

1. `git clone https://github.com/OscarVanL/COMP6254-IoT-Systems/`

2. `cd COMP6254-IoT-Systems`

3. `conda create --name COMP6253-IoT-Systems python=3.8`

4. `conda activate COMP6253-IoT-Systems`

5. `pip install -r requirements.txt`

6. Add relevant credentials to `coursework/CW_Mqtt_Secrets.yaml` and `labs/Lab2_Mqtt_Secrets.yaml`

7. `python main.py --lab 2` or `python main.py --coursework` 

## Setup in Docker

1. `git clone https://github.com/OscarVanL/COMP6254-IoT-Systems/`

2. `cd COMP6254-IoT-Systems`

3. Add relevant credentials to `coursework/CW_Mqtt_Secrets.yaml` and `labs/Lab2_Mqtt_Secrets.yaml`

4. `./build_docker.sh`

5. `./run_docker.sh` (note: Edit the run command in this script to change between the lab or coursework project)