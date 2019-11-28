def verify_instrument(instr=''):
    """ Verify the instrument is an allowed value """

    #todo: move this to instrument classes?
    assert instr != '', 'instrument value is blank'
    allowed_instruments = ['DEIMOS', 'ESI', 'HIRES', 'KCWI', 'LRIS', 'MOSFIRE', 'NIRC2', 'NIRES', 'NIRSPEC', 'OSIRIS'];
    assert instr in allowed_instruments, 'unknown instrument value'

def verify_date(date=''):
    """
    Verify that date value has format yyyy-mm-dd
        yyyy >= 1990
        mm between 01 and 12
        dd between 01 and 31
    """
    
    import re
    
    # Verify correct format (yyyy-mm-dd or yyyy/mm/dd)

    assert date != '', 'date value is blank'
    assert re.search('\d\d\d\d[-/]\d\d[-/]\d\d', date), 'unknown date format'
    
    # Switch to yyyy-mm-dd format and split into individual elements
    
    date = date.replace('/', '-')
    print(date)
    year, month, day = date.split('-')
    print(year, month, day)
    
    # Year must be 1990 or larger
    
    assert int(year) >= 1990, 'year value must be 1990 or larger'
    
    # Month must be between 1 and 12
    
    assert int(month) >= 1 and int(month) <= 12, 'month value must be between 1 and 12'
    
    # Day value must be between 1 and 31
    
    assert int(day) >= 1 and int(day) <= 31, 'day value must be between 1 and 31'

def verify_utc(utc=''):
    """
    Verify that utc value has the format hh:mm:ss[.ss]
    
    hh between 0 and 24
    mm between 0 and 60
    ss between 0 and 60
    """
    
    import re
    
    # Verify correct format (hh:mm:ss[.ss])
    
    assert utc != '', 'utc is blank'
    assert re.search('\d\d:\d\d:\d\d', utc), 'unknown utc format'
    
    # Split into individual elements
    
    hour, minute, second = utc.split(':')
    
    # hour must be between 0 and 24
    
    assert int(hour) >= 0 and int(hour) <= 24, 'hour value must be between 0 and 24'
    
    # minute must be between 0 and 60
    
    assert int(minute) >= 0 and int(minute) <= 60, 'minute value must be between 0 and 60'
    
    # second must be between 0 and 60
    
    assert float(second) >= 0 and float(second) <= 60, 'second value must be between 0 and 60'
