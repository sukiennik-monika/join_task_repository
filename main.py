import join_task
import os

while True:
    command = input(f'Enter the joining command below:\n')
    if command[0:5] == "join " and len(command) > 5:
        command = command.split()
        join_task.join(command[1], command[2], command[3], command[4])
        print(f'\nJoining result was saved in path: {os.path.dirname(os.path.abspath(command[1]))}')
        break
    else:
        print(f'Wrong joining command, try again!\n')

end=input(f"\nPress Enter to end program")