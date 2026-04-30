from datetime import datetime, timedelta

from automations.run_RECON_file import run_RECON_file
from automations.run_SWIM_file import run_SWIM_file

def main():
    try:
        run_SWIM_file()
        
    except Exception as e:
        print(e)

    try:
        
        run_RECON_file()
        
    except Exception as e:
        print(e)
    
if __name__ == '__main__':
    main()