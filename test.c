#include <stddef.h>

#include "xlogtranslate.h"

int main() {
	Result *result, *current;
	int i;

	for (i = 0; i < 1000000; i++) {
		result = parseWalFile("000000010000000000000001", 17812976);
		current = result;
		while (current != NULL) {
			// printf("%d %d\n", result->xlogid, result->xrecoff);
			current = (Result *) current->next;
		}

		freeWalResult(result);
	}

	return 0;
}