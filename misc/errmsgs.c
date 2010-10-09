/*
 * Copyright (c) 2009,2010 Hajime Nakagami<nakagami@gmail.com>
 * All rights reserved.
 * Licensed under the New BSD License
 * (http://www.freebsd.org/copyright/freebsd-license.html)
 *
 */
#include <stdio.h>
#define	SLONG long
#define SCHAR char

/*http://firebird.cvs.sf.net/viewvc/firebird/firebird2/src/include/gen/msgs.h*/
#include "msgs.h"   

int main(int argc, char *argv[])
{
    int i;
    FILE *fp = fopen("fberrmsgs.py", "w");

    fprintf(fp, "\
#############################################################################\n\
# The contents of this file are subject to the Interbase Public\n\
# License Version 1.0 (the \"License\"); you may not use this file\n\
# except in compliance with the License. You may obtain a copy\n\
# of the License at http://www.Inprise.com/IPL.html\n\
# \n\
# Software distributed under the License is distributed on an\n\
# \"AS IS\" basis, WITHOUT WARRANTY OF ANY KIND, either express\n\
# or implied. See the License for the specific language governing\n\
# rights and limitations under the License.\n\n");
    fprintf(fp, "messages = {\n");
    for (i = 0; messages[i].code_text; i++) {
        fprintf(fp, "    %d : '''%s\\n''', \n", messages[i].code_number, messages[i].code_text);
    }
    fprintf(fp, "}\n");

    fclose(fp);
    return 0;
}
