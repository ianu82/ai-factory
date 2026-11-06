"""Own the new console volume, then drop privileges before opening HTTP."""
import os
import json
import stat
import time
from pathlib import Path
from .server import main

if __name__=='__main__':
    directory=Path(os.environ.get('FACTORY_CONSOLE_DB','/data/console.db')).parent
    if os.getuid()==0:
        directory.mkdir(mode=0o700,parents=True,exist_ok=True)
        os.chown(directory,10001,10001)
        if not all(os.environ.get(k) for k in ('FACTORY_CONSOLE_ACCESS_KEY','FACTORY_CONSOLE_BRIDGE_TOKEN')):
            path=directory/'credentials.json'
            deadline=time.monotonic()+1200
            print('Waiting for private console configuration on the console volume',flush=True)
            while not path.exists() and time.monotonic()<deadline:
                time.sleep(1)
            info=path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_size>2048:
                raise ValueError('Unsupported console credential configuration')
            document=json.loads(path.read_text())
            for key in ('FACTORY_CONSOLE_ACCESS_KEY','FACTORY_CONSOLE_BRIDGE_TOKEN'):
                value=document.get(key)
                if not isinstance(value,str) or not 20<=len(value)<=128:
                    raise ValueError('Invalid private console configuration')
                os.environ.setdefault(key,value)
            os.chown(path,10001,10001)
            os.chmod(path,0o600)
        os.setgroups([])
        os.setgid(10001)
        os.setuid(10001)
    main()
