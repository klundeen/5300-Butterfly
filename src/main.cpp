// SQL DB main program file
// Dominic Burgi
// CPSC 5300 - Butterfly

#include "sql_shell.h"
#include <string.h>

#define DEBUG_ENABLED
#include "debug.h"

// Constants
// const char *HOME = "cpsc5300/butterfly_data";
std::string const USAGE = "Program requires 1 argument, the path to a writeable directory\n";

// Helper functions
static bool DataDirExists(std::string data_dir);
static void GetArgs(int argc, char *argv[], std::string &data_dir);

int main(int argc, char *argv[])
{
    DEBUG_OUT("Start of main()\n");
    std::string data_dir;
    GetArgs(argc, argv, data_dir);

    if(!DataDirExists(data_dir))
    {
        std::cout << "Data dir must exist... Exiting.\n" << std::endl;
        return -1;
    }

    const char *home = std::getenv("HOME");
    std::string envdir = std::string(home) + "/" + data_dir;
    DEBUG_OUT_VAR("Initializing DB environment in %s\n", envdir.c_str());

    DEBUG_OUT("Running SQL Shell\n");
    SqlShell sql_shell;
    sql_shell.InitializeDbEnv(envdir);
    sql_shell.Run();
    return 0;
}

static void GetArgs(int argc, char *argv[], std::string &data_dir) {
    if (argc <= 1 || argc >= 3) {
        std::cerr << USAGE;
        std::exit(1);
    }

    data_dir = argv[1];
}

static bool DataDirExists(std::string data_dir)
{
    std::cout << "Have you created a dir: " << data_dir  << "? (y/n) " << std::endl;
    std::string ans;
    getline(std::cin, ans);
    if( ans[0] != 'y')
    {
        return false;
    }
    return true;
}