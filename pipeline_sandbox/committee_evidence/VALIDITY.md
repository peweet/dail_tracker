# Cross-match VALIDITY probe

_For each witness org: the RAW records its normalised key joins to, per dataset. `amb` = # distinct raw names sharing that key (>1 ⇒ key not unique on that side). Judge same-entity by eye; flag generic/short keys._


**13/43 distinct witness orgs match ≥1 dataset.** Validity (not count) is the question below.


### An Coimisiún Pleanála
`key='AN COIMISIUN PLEANALA'` tokens=3
- **payments_payee** (amb=2  ⚠️AMBIGUOUS): AN COIMISIÚN PLEANÁLA; An Coimisiun Pleanala

### Beaumont Hospital
`key='BEAUMONT HOSPITAL'` tokens=2
- **payments_publisher** (amb=1): Beaumont Hospital

### Department of Education and Youth
`key='DEPARTMENT EDUCATION YOUTH'` tokens=3
- **payments_payee** (amb=1): DEPARTMENT OF EDUCATION AND YOUTH
- **payments_publisher** (amb=1): Department of Education and Youth

### Department of Finance
`key='DEPARTMENT FINANCE'` tokens=2
- **payments_publisher** (amb=1): Department of Finance

### Department of Public Expenditure
`key='DEPARTMENT PUBLIC EXPENDITURE'` tokens=3
- **payments_payee** (amb=1): DEPARTMENT OF PUBLIC EXPENDITURE AND

### Department of Social Protection
`key='DEPARTMENT SOCIAL PROTECTION'` tokens=3
- **payments_publisher** (amb=1): Department of Social Protection

### Dublin City Council
`key='DUBLIN CITY COUNCIL'` tokens=3
- **payments_payee** (amb=2  ⚠️AMBIGUOUS): DUBLIN CITY COUNCIL; Dublin City Council
- **councils** (amb=1): Dublin City Council

### Education
`key='EDUCATION'` tokens=1 ⚠️SHORT/GENERIC
- **payments_payee** (amb=1): EDUCATION LTD

### Housing Agency
`key='HOUSING AGENCY'` tokens=2
- **payments_payee** (amb=2  ⚠️AMBIGUOUS): HOUSING AGENCY; Housing Agency

### National Association of Regional Game Councils
`key='NATIONAL ASSOCIATION REGIONAL GAME COUNCILS'` tokens=5
- **lobby_registrant** (amb=1): National Association of Regional Game Councils
- **lobby_client** (amb=1): National Association of Regional Game Councils

### National Treasury Management Agency
`key='NATIONAL TREASURY MANAGEMENT AGENCY'` tokens=4
- **payments_payee** (amb=1): NATIONAL TREASURY MANAGEMENT AGENCY

### Office of Public Works
`key='OFFICE PUBLIC WORKS'` tokens=3
- **payments_payee** (amb=4  ⚠️AMBIGUOUS): OFFICE OF PUBLIC WORKS; Office Of Public Works; Office of Public Works; THE OFFICE OF PUBLIC WORKS
- **payments_publisher** (amb=1): Office of Public Works

### Tailte Éireann
`key='TAILTE EIREANN'` tokens=2
- **payments_payee** (amb=4  ⚠️AMBIGUOUS): TAILTE EIREANN; TAILTE ÉIREANN; Tailte Eireann; Tailte Éireann
- **payments_publisher** (amb=1): Tailte Éireann
