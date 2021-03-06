"""
WLCG data processing.

Author: Henrik Thostrup Jensen <htj@ndgf.org>

Copyright: Nordic Data Grid Facility (2011)
"""


# constants for accessing values in records
YEAR        = 'year'
MONTH       = 'month'
TIER        = 'tier'
HOST        = 'host'

VO_NAME     = 'vo_name'
VO_GROUP    = 'vo_group'
VO_ROLE     = 'vo_role'
USER        = 'user'

N_JOBS      = 'n_jobs'
CPU_TIME    = 'cpu_time'
WALL_TIME   = 'wall_time'
KSI2K_CPU_TIME  = 'ksi2k_cpu_time'
KSI2K_WALL_TIME = 'ksi2k_wall_time'
EFFICIENCY  = 'efficiency'

CPU_EQUIVALENTS        = 'cpu_equivalents'
WALL_EQUIVALENTS       = 'wall_equivalents'
KSI2K_CPU_EQUIVALENTS  = 'ksi2k_cpu_equivalents'
KSI2K_WALL_EQUIVALENTS = 'ksi2k_wall_equivalents'

DATE_START  = 'date_start'
DATE_END    = 'date_end'

FIELDS = ( YEAR, MONTH, TIER, HOST, VO_NAME, VO_GROUP, VO_ROLE, USER, N_JOBS, CPU_TIME, WALL_TIME, KSI2K_CPU_TIME, KSI2K_WALL_TIME, EFFICIENCY )
KEY_FIELDS = ( YEAR, MONTH, TIER, HOST, VO_NAME, VO_GROUP, VO_ROLE, USER )

DEFAULT_SCALE_FACTOR = 1.75



def rowsToDicts(rows):
    """
    Convert rows from database into dicts, which are much nicer to work with.
    """
    def decimalConvert(d):
        if d is None:
            return d
        else:
            return float(d)


    dicts = []

    for row in rows:
        #year, month, host, user, vo_name, vo_group, vo_role, n_jobs, cputime, walltime = row
        year, month, host, vo_name, vo_group, vo_role, user, n_jobs, cputime, walltime, cputime_scaled, walltime_scaled = row

        d = { YEAR : year, MONTH : month, HOST : host,
              VO_NAME : vo_name, VO_GROUP : vo_group, VO_ROLE : vo_role, USER : user,
              N_JOBS : n_jobs, CPU_TIME : decimalConvert(cputime), WALL_TIME : decimalConvert(walltime),
              KSI2K_CPU_TIME : decimalConvert(cputime_scaled), KSI2K_WALL_TIME : decimalConvert(walltime_scaled)
            }
        dicts.append(d)

    return dicts



def sortKey(record, field_order=None):
    """
    Given a record, returns a key usable for sorting.
    """
    if field_order is None:
        field_order = FIELDS

    attrs = []
    for f in field_order:
        if f in record:
            attrs.append(record[f])
    return tuple(attrs)



def findMissingScaleFactors(records):

    missing_scale_factors = {}
    for rec in records:
        if rec[KSI2K_CPU_TIME] is None or rec[KSI2K_WALL_TIME] is None:
            missing_scale_factors[rec[HOST]] = True

    return missing_scale_factors.keys()



def addMissingScaleValues(records, scale_factor=DEFAULT_SCALE_FACTOR):
    """
    Add scaled cputime and walltime values which did not come with a scaling value.
    """
    # note: the magic factor for converting ksi2k factors to
    # hs06 factors is to multiply with 4, i.e., ksi2k*4 = hs06

    for r in records:
        if r[KSI2K_CPU_TIME] is None:
            r[KSI2K_CPU_TIME]  = r[CPU_TIME]  * scale_factor
        if r[KSI2K_WALL_TIME] is None:
            r[KSI2K_WALL_TIME] = r[WALL_TIME] * scale_factor

    return records



def mergeRecords(records):
    """
    Merge two or more records, keeping their base information, but adding count variables.
    Assumes base information is identical.
    """
    def sumfield(dicts, field):
        fields = [ d[field] for d in dicts ]
        result = sum(fields) if not None in fields else None
        return result

    nr = records[0].copy()
    nr[N_JOBS]          = sumfield(records, N_JOBS)
    nr[CPU_TIME]        = sumfield(records, CPU_TIME)
    nr[KSI2K_CPU_TIME]  = sumfield(records, KSI2K_CPU_TIME)
    nr[WALL_TIME]       = sumfield(records, WALL_TIME)
    nr[KSI2K_WALL_TIME] = sumfield(records, KSI2K_WALL_TIME)

    return nr



def createFieldKey(record):
    """
    Returns the key fields of a record as a tuple.
    """
    key = tuple ( [ record[field] for field in KEY_FIELDS if field in record ] )
    return key



def collapseFields(records, collapse_fields):
    """
    Removes one or more key fields in the records and sums togther the records
    into a new batch of records for the new shared key fields.
    """
    for cf in collapse_fields:
        assert cf in KEY_FIELDS, 'Cannot collapse non-existing field %s' % cf

    collapsed_records = {}

    for rec in records:
        r = rec.copy()
        for cf in collapse_fields:
            del r[cf]
        key = createFieldKey(r)
        collapsed_records.setdefault(key, []).append(r)

    summed_records = []
    for records in collapsed_records.values():
        summed_records.append( mergeRecords(records) )

    return summed_records



def addEffiencyProperty(records):
    """
    Adds an effiency attribute to a list of records.
    """
    for rec in records:
        if rec[WALL_TIME] < 1:
            rec[EFFICIENCY] = '-'
        else:
            rec[EFFICIENCY] = int(rec[CPU_TIME] * 100 / float(rec[WALL_TIME]))

    return records



def addEquivalentProperties(records, days):
    """
    Adds machine equivalents to records.
    """
    divisor = 24 * days

    for rec in records:
        rec[CPU_EQUIVALENTS]  = int( rec[CPU_TIME] / divisor )
        rec[WALL_EQUIVALENTS] = int( rec[WALL_TIME] / divisor )
        if KSI2K_CPU_TIME in rec:
            rec[KSI2K_CPU_EQUIVALENTS] = int(rec[KSI2K_CPU_TIME] / divisor)
        if KSI2K_WALL_TIME in rec:
            rec[KSI2K_WALL_EQUIVALENTS] = int(rec[KSI2K_WALL_TIME] / divisor)

    return records



def tierMergeSplit(records, tier_mapping, tier_shares, default_tier):

    def mapHostToTier(host):
        try:
            return str(tier_mapping[host])
        except KeyError:
            print "WARNING: No tier mapping for host %s" % host
            return default_tier
#        # using heuristic
#        if host.endswith('.no'):
#            tier = NORWAY_TIER2
#        elif host.endswith('.se'):
#            tier = SWEDEN_TIER2
#        elif host.endswith('.fi'):
#            tier = CSC_TIER
#        else:
#            tier = DEFAULT_TIER
#        return tier


    def ruleMatch(rule, record):
        for key, value in rule.items():
            try:
                if record[key] != value:
                    return False
            except KeyError:
                return False
        return True


    def applyRatio(record, ratio):
        record[N_JOBS]          = int(record[N_JOBS]      * ratio)
        record[CPU_TIME]        = record[CPU_TIME]        * ratio
        record[WALL_TIME]       = record[WALL_TIME]       * ratio
        record[KSI2K_CPU_TIME]  = record[KSI2K_CPU_TIME]  * ratio if KSI2K_CPU_TIME  in record else None
        record[KSI2K_WALL_TIME] = record[KSI2K_WALL_TIME] * ratio if KSI2K_WALL_TIME in record else None
        return record


    # start of function
    tr = {}

    for r in records:

        rc = r.copy()
        rc[TIER] = mapHostToTier(r[HOST])

        match = False
        for rule, ratio in tier_shares:
            if ruleMatch(rule, rc):
                match = True
                break

        # we don't split entries if there are less than 10 jobs,
        # as it creates small meaningless entries, which are just noise
        # also, if ratio is 0 or 1 skip the check as it is meaningless
        if match and rc[N_JOBS] > 10:
            #print "SPLIT", rc[TIER], rc[USERSN], rc[VO_NAME], rc[VO_GROUP], rc[VO_ROLE], rc[N_JOBS]
            t2_ratio = ratio
            t1_ratio = 1 - t2_ratio

            if t1_ratio != 0:
                t1_base_record = rc.copy()
                t1_base_record.update( {TIER: default_tier} )

                t1k = createFieldKey(t1_base_record)
                t1_record = applyRatio(t1_base_record, t1_ratio)
                tr.setdefault(t1k, []).append(t1_record)

            if t2_ratio != 0:
                t2k = createFieldKey(rc)
                t2_record = applyRatio(rc, t2_ratio)
                tr.setdefault(t2k, []).append(t2_record)

        else:
            r_key = createFieldKey(rc)
            tr.setdefault(r_key, []).append( applyRatio(rc, 1) )


    tier_records = []

    for t_recs in tr.values():
        tier_records.append( mergeRecords(t_recs) )

    return tier_records



def splitRecords(records, split_attribute):

    assert split_attribute in KEY_FIELDS, 'Invalid split attribute specified'

    split_records = {}

    for rec in records:

        rc = rec.copy()
        split_records.setdefault(rc[split_attribute], []).append(rc)
        del rc[split_attribute]

    return split_records

