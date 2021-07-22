## Overview

Everything relies on postgresql on both data storage and string matching function since it's easy and the fastest solution to test the idea.

There are 2 parts here.

1. `push2db.py` -- obviously push both `***_public_t0**` and `"60"` to database for further processes. All company information will be push to `"company"`
2. `calc_sim.py` -- finding if there are any possible duplicates between 2 dataset.
    - use company from `***_public_t0**` as reference
    - find any company that might be a duplicate of the item above which has criterias as follows
        - same province or zipcode
        - similarity [1] score > 0.55
        - Levenshtein [2] distances < 12

- [1] pg_trgm https://www.postgresql.org/docs/13/pgtrgm.html
- [2] fuzzystrmatch https://www.postgresql.org/docs/13/fuzzystrmatch.html

## checkout possible duplicate result

    select c1.name as name1, c2.name as name2, d.diff, d.similarity, c1.dbd_id, c2.c_code
    FROM possible_dup d
    LEFT JOIN company c1 on d.parent_id = c1.id
    LEFT JOIN company c2 on d.pair_id = c2.id
    ORDER BY d.parent_id ASC, d.similarity DESC, d.diff ASC;