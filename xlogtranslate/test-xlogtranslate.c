#include <stddef.h>
#include <stdio.h>

#include "xlogtranslate.h"

int main() {
	Result *result, *current;

	result = parseWalFile("000000010000000000000001", 17812976);
	current = result;
	while (current != NULL) {
		printf("%d %d\n", current->xlogid, current->xrecoff);
		current = (Result *) current->next;
	}

	freeWalResult(result);

	return 0;
}