#!/usr/bin/perl -w
use strict;
use warnings;
use threads;
use threads ('yield','stack_size' => 128*16384);
#use Date::Parse;
#use POSIX qw(strftime);

#Enable Autoflush
select(STDERR);
local $| = 1;
select(STDOUT);
local $| = 1;

#Maximum Parallel Threads
my $thread_limit = 200;

#Waiting time between checking new task and starting the next thread
my $waiting_seconds = 5;

my $elapsed;
my $start = time - $waiting_seconds;

#Main Loop
for ( ; ; )
{

    my $thread_count = threads -> list ();
    my @joinable_threads = threads -> list ( threads::joinable );

    if ( $thread_count < $thread_limit )
    {
        if ( waiting () == 0 )
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
                sleep 1;
            }
        }
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
    if ( $rnd > 5000 ) { return 1; } else { return 0; }
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
    my $now = format_timestamp ( time () );
    print ( $now . " " . $message . "\n" );
}

sub format_timestamp
{
    my ( $timestamp ) = @_;
    my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = gmtime ( $timestamp );
    my $result = sprintf ( "%d-%02d-%02dT%02d:%02d:%02dZ", ( $year + 1900 ), ( $mon + 1 ), $mday, $hour, $min, $sec );
    return $result;
}

sub waiting
{
    if ( $waiting_seconds == 0 ) { return 0; }
    $elapsed = time - $start;
    if ( $elapsed >= $waiting_seconds )
    {
        $elapsed = 0;
        $start = time;
        return 0;
    }
    else
    {
        return 1;
    }
}