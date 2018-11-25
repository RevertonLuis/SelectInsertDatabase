def load_settings(file_name):

    """ Routine that load the project settings """

    # load the file
    file = open(file_name)
    lines = file.readlines()
    file.close()

    # dictionary in the form
    # 1 column          = key
    # 2, 3, ... column  = values list
    settings = {}
    for line in lines:

        # Check the line columns number and if it's not a commentary
        if len(line.split()) > 0 and line[0] != '#':

            # Creating the pair key-value
            settings[line.split()[0]] = [str(x) for x in line.split()[1:]]
    return settings
