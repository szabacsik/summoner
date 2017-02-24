#!/usr/bin/perl -w
use strict;
use warnings;
use threads;
use threads qw(yield);
#use Date::Parse;
#use POSIX qw(strftime);

#Enable Autoflush
select(STDERR);
local $| = 1;
select(STDOUT);
local $| = 1;

#Maximum Parallel Threads
my $thread_limit = 200;

#Main Loop
for ( ; ; )
{
    my $thread_count = threads -> list ();
    my @joinable_threads = threads -> list ( threads::joinable );

    if ( $thread_count < $thread_limit )
    {
        if ( new_task_awaits () == 1 )
        {
            my $thread = async { start_new_task ( new_task_parameter () ) };
            if ( my $error = $thread -> error () )
            {
                message ( "Thread error: $error" );
            }
            else
            {
                message ( "Thread " . $thread -> tid () . " has been started." );
            }
        };
    }

    foreach ( @joinable_threads )
    {
        $_ -> join ();
        message ( "Thread " . $_ -> tid () . " has been finished." );
        message ( statistics () );
    }

    threads -> yield ();
    yield ();

}

sub new_task_awaits
{
    my $rnd = int ( rand ( 10001 ) );
    if ( $rnd > 9999 ) { return 1; } else { return 0; }
}

sub new_task_parameter
{
    return int ( rand ( 30 ) + 1 );
}

sub start_new_task
{
    my @args = @_;
    my $command = 'printf `date +%H:%M:%S.%N`; sleep ' . $_[0] . '; printf -; printf `date +%H:%M:%S.%N`';
    my $output = `$command`;
    message ( "Finished thread's output: " . $output );
}

sub statistics
{
    my $thread_count = threads -> list ();
    my @running_threads = threads -> list ( threads::running );
    my $running_thread_count = scalar keys @running_threads;
    my @joinable_threads = threads -> list ( threads::joinable );
    my $joinable_thread_count = scalar keys @joinable_threads;
    my $message = "Threads limit: " . $thread_limit . ", total: " . $thread_count . ", running: " . $running_thread_count . ", joinable: " . $joinable_thread_count;
    return $message;
}

sub message
{
    my @args = @_;
    my $message = $_[0];
    my $now = formatdate ( time () - 1 );
    print ( $now . " " . $message . "\n" );
}

sub formatdate
{
    my ( $ts ) = @_;
    my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = gmtime ( $ts );
    my $retval = sprintf ( "%d-%02d-%02dT%02d:%02d:%02dZ", ( $year + 1900 ), ( $mon + 1 ), $mday, $hour, $min, $sec );
    return $retval;
}
