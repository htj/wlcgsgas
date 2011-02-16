"""
Query construction for wlcgsgas library

Author: Henrik Thostrup Jensen <htj@ndgf.org>

Copyright: Nordic Data Grid Facility (2011)
"""

# This query selects what is to the best of my knowledge relevant data for
# WLCG, repairs known issues, and then groups it together.
#
# Start and end data must be supplied when using the query.
#
# The resulting data will usually need further processing for scaling, tier-mapping
# and aggregation.

WLCG_QUERY = """
SELECT
    extract(YEAR FROM execution_time)::integer AS year,
    extract(MONTH FROM execution_time)::integer AS month,
    machine_name,
    CASE WHEN vo_name IS NULL THEN
         CASE WHEN user_identity = '/C=SI/O=SiGNET/O=IJS/OU=F9/CN=Andrej Filipcic' THEN 'atlas'
              WHEN user_identity = 'aliprod' OR user_identity LIKE '/C=ch/O=AliEn/OU=ALICE/CN%%' THEN 'alice'
         END
         ELSE CASE WHEN vo_name = 'atlas.cern.ch' THEN 'atlas'
              ELSE vo_name
              END
    END AS vo_name,
    vo_group,
    vo_role,
    CASE WHEN user_identity LIKE '/C=ch/O=AliEn/OU=ALICE/CN%%' THEN 'aliprod'
         ELSE user_identity
    END AS user_identity,
    sum(n_jobs)   AS n_jobs,
    sum(cputime) AS cputime,
    sum(walltime) AS walltime,
    sum(cputime_scaled) as cputime_scaled,
    sum(walltime_scaled) as walltime_scaled
FROM
    uraggregated
WHERE
    execution_time >= %s AND
    execution_time <= %s AND
    ( vo_name   IN ('atlas', 'alice', 'cms') OR
      vo_issuer IN ('/DC=ch/DC=cern/OU=computers/CN=voms.cern.ch', '/DC=ch/DC=cern/OU=computers/CN=lcg-voms.cern.ch') OR
      user_identity IN ('aliprod', '/C=SI/O=SiGNET/O=IJS/OU=F9/CN=Andrej Filipcic') OR
      user_identity LIKE '/C=ch/O=AliEn/OU=ALICE/CN%%'
    )
GROUP BY
    year, month, machine_name, user_identity, vo_name, vo_group, vo_role
ORDER BY
    year, month, machine_name, user_identity, vo_name, vo_group, vo_role
;
"""

