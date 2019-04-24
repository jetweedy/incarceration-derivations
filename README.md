# incarceration-derivations

This is a sample of a SQL script to derive incarceration spans from daily jail records that were being scraped from county records sites, email attachments and uploads from local sherriff's offices. It was written in order to drastically speed up an initially web-based daily derivation process written in Laravel that was connecting repeatedly as part of a scheduled job. Initially taking anywhere from about 1 to 5 minutes depending on county size, the script is now able to handle even the largest counties in just a few seconds each day. It was able to run the largest counties over the course of an entire year's worth of records in about 1.5-2hrs, making it possible to retroactively correct and speed up the derivation process in a few hours instead of over the course of days or weeks of continuous processing.
