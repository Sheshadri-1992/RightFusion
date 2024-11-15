import os
import time
import subprocess
import shlex

for i in range(0,10):
  start_time = time.time()
  command = "docker update func1 --cpus=3 --cpuset-cpus=0,1,5"
  # os.system("docker update func1 --cpus=3 --cpuset-cpus=0,3,5")
  subprocess.run(shlex.split(command))
  end_time = time.time()
  total_time = (end_time - start_time)*1000
  print(total_time)