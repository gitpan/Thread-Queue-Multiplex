use strict;
use warnings;
use vars qw($testno $loaded $srvtype);
BEGIN {
	my $tests = 137;
	print STDERR "Note: some tests have significant delays...\n";
	$^W= 1;
	$| = 1;
	print "1..$tests\n";
}

END {print "not ok $testno\n" unless $loaded;}

package Qable;
use Thread::Queue::Queueable;

use base qw(Thread::Queue::Queueable);

sub new {
	return bless {}, shift;
}

sub onEnqueue {
	my $obj = shift;
	my $class = ref $obj;
#	print STDERR "$class object enqueued\n";
	return $obj->SUPER::onEnqueue;
}

sub onDequeue {
	my ($class, $obj) = @_;
#	print STDERR "$class object dequeued\n";
	return $class->SUPER::onDequeue($obj);
}

sub onCancel {
	my $obj = shift;
#	print STDERR "Item cancelled.\n";
	1;
}

sub curse {
	my $obj = shift;
	return $obj->SUPER::curse;
}

sub redeem {
	my ($class, $obj) = @_;
	return $class->SUPER::redeem($obj);
}

1;

package SharedQable;
use Thread::Queue::Queueable;

use base qw(Thread::Queue::Queueable);

sub new {
	my %obj : shared = ( Value => 1);
	return bless \%obj, shift;
}

sub set_value {
	my $obj = shift;
	$obj->{Value}++;
	return 1;
}

sub get_value { return shift->{Value}; }

sub redeem {
	my ($class, $obj) = @_;
	return bless $obj, $class;
}

1;

package main;

use threads;
use threads::shared;
use Thread::Queue::Multiplex;

$srvtype = 'init';

sub has_all_subs {
	my $results = shift;
	my $subs = shift;

	$subs = [ keys %$results ] unless ref $subs;

	foreach my $id (@$subs) {
		return undef
			unless exists $results->{$id};
		my $result = $results->{$id};
		return undef unless (scalar @$result == scalar @_);
		foreach (0..$#$result) {
			return undef
				if (ref $_[$_]) &&
					((! ref $result->[$_]) ||
						(ref $_[$_] ne ref $result->[$_]));

			return undef
				unless (ref $_[$_]) || ($result->[$_] eq $_[$_]);
		}
	}
#
#	validate any result values for each sub
#
	return 1;
}

sub report_result {
	my ($result, $testmsg, $okmsg, $notokmsg) = @_;

	if ($result) {

		$okmsg = '' unless $okmsg;
		print STDOUT (($result eq 'skip') ?
			"ok $testno # skip $testmsg for $srvtype\n" :
			"ok $testno # $testmsg $okmsg for $srvtype\n");
	}
	else {
		$notokmsg = '' unless $notokmsg;
		print STDOUT
			"not ok $testno # $testmsg $notokmsg for $srvtype\n";
	}
	$testno++;
}
#
#	test normal dequeue method
#
sub run_dq {
	my $q = shift;

	$q->subscribe;
	while (1) {
		my $left = $q->pending;

		my $req = $q->dequeue;
#print threads->self()->tid(), " run_dq dq'd\n";
		my $id = shift @$req;

		if ($req->[0] eq 'stop') {
			$q->respond($id, 'stopped');
			$q->unsubscribe();
			last;
		}

		if ($req->[0] eq 'wait') {
			sleep($req->[1]);
		}

		if ($req->[1] && ref $req->[1] && (ref $req->[1] eq 'SharedQable')) {
			$req->[1]->set_value();
		}
#
#	ignore simplex msgs
#
		next
			unless $id;

		$q->marked($id) ?
			$q->respond($id, $q->get_mark($id)) :
			$q->respond($id, @$req);
	}
}
#
#	test nonblocking dequeue method
#
sub run_nb {
	my $q = shift;

	$q->subscribe;
	while (1) {
		my $req = $q->dequeue_nb;

		sleep 1, next
			unless $req;

#print "run_nb dq'd\n";
		my $id = shift @$req;

		$q->unsubscribe(),
		$q->respond($id, 'stopped'),
		last
			if ($req->[0] eq 'stop');

#print STDERR join(', ', @$req), "\n";
		sleep($req->[1])
			if ($req->[0] eq 'wait');
#
#	ignore simplex msgs
#
		$q->respond($id, @$req)
			if $id;
	}
}
#
#	test timed dequeue method
#
sub run_until {
	my $q = shift;

	my $timeout = 2;
	$q->subscribe;
	while (1) {

		my $req = $q->dequeue_until($timeout);
		sleep 1, next
			unless $req;

#print "run_until dq'd\n";
		my $id = shift @$req;

		$q->unsubscribe(),
		$q->respond($id, 'stopped'),
		last
			if ($req->[0] eq 'stop');

		sleep($req->[1])
			if ($req->[0] eq 'wait');
#
#	ignore simplex msgs
#
		$q->respond($id, @$req)
			if $id;
	}
}
#
#	acts as a requestor thread for class-level
#	wait tests
#
sub run_requestor {
	my $q = shift;

	$q->wait_for_subscribers(1);

	while (1) {
		my $id = $q->publish('request');
		my $resp = $q->wait($id);

		($resp) = values %$resp;
		last
			if ($resp->[0] eq 'stop');
	}
	return 1;
}
#
#	test urgent dequeue method
#
sub run_urgent {
	my $q = shift;

	$q->subscribe;
	while (1) {
		my $req = $q->dequeue_urgent;

		sleep 1, next
			unless $req;

#print "run_urgent dq'd\n";
		my $id = shift @$req;

		$q->unsubscribe(),
		$q->respond($id, 'stopped'),
		last
			if ($req->[0] eq 'stop');

		sleep($req->[1])
			if ($req->[0] eq 'wait');
#
#	ignore simplex msgs
#
		$q->respond($id, @$req)
			if $id;
	}
}

$testno = 1;

report_result(1, 'load module');
#
#	create queue
#	spawn server thread
#	execute various requests
#	verify responses
#
#	test constructor
#
my $q = Thread::Queue::Multiplex->new(ListenerRequired => 1);

report_result(defined($q), 'create queue');
#
#	test different kinds of dequeue
#
my @servers = (\&run_dq, \&run_nb, \&run_until);
my @types = ('normal', 'nonblock', 'timed');

my ($result, $id, $server);

my $start = $ARGV[0] || 0;
my $qable = Qable->new();
my $sharedqable = SharedQable->new();
my $srvcount = 4;

foreach ($start..$#servers) {
#
#	create subscriber threads
#
	my @subs = ();
	foreach my $i (1..$srvcount) {
		push @subs, threads->new($servers[$_], $q);
	}
	$srvtype = $types[$_];
#
#	wait for all subscribers
#
	report_result($q->wait_for_subscribers($srvcount), 'wait_for_subscribers()');
#
#	test get_subscribers
#
	my @subids = $q->get_subscribers();
	report_result((@subids && (scalar @subids == $srvcount)), 'get_subscribers()');

####################################################################
#
#	BEGIN PUBLISH TESTS
#
####################################################################
#
#	test publish_simplex
#
	$id = $q->publish_simplex('foo', 'bar');
	report_result(defined($id), 'publish_simplex()');
#
#	test publish
#
	$id = $q->publish('foo', 'bar');
	report_result(defined($id), 'publish()');
#
#	test ready(); don't care about outcome
#	(prolly need eval here)
#
	$result = $q->ready($id);
	report_result(1, 'ready()');
#
#	test wait()
#
	$result = $q->wait($id);
	report_result((defined($result) && has_all_subs($result, \@subids, 'foo', 'bar')),
		'wait()');
#
#	test dequeue_response
#
	$id = $q->publish('foo', 'bar');
	$result = $q->dequeue_response($id);
	report_result((defined($result) && has_all_subs($result, \@subids, 'foo', 'bar')),
		'dequeue_response()');
#
#	test Queueable publish
#
	$id = $q->publish('foo', $qable);
	report_result(defined($id), 'publish() Queueable');

	$result = $q->wait($id);
	report_result((defined($result) && has_all_subs($result, \@subids, 'foo', $qable)),
		'wait() Queueable');
#
#	test wait_until, publish_urgent
#
	$id = $q->publish('wait', 3);
	my $id1 = $q->publish('foo', 'bar');
	$result = $q->wait_until($id, 1);
	report_result((!defined($result)), 'wait_until() expires');

	my $id2 = $q->publish_urgent('urgent', 'entry');
#
#	should get wait reply here
#
	$result = $q->wait_until($id, 5);
	report_result((defined($result) && has_all_subs($result, \@subids, 'wait', 3)), 'wait_until()');
#
#	should get urgent reply here
#
	$result = $q->wait($id2);
	report_result((defined($result) && has_all_subs($result, \@subids, 'urgent', 'entry')), 'publish_urgent()');
#
#	should get normal reply here
#
	$result = $q->wait($id1);
	report_result((defined($result) && has_all_subs($result, \@subids, 'foo', 'bar')),  'publish()');
#
#	test wait_any: need to queue up several
#
	my %ids = ();

	map { $ids{$q->publish('foo', 'bar')} = 1; } (1..10);
#
#	repeat here until all ids respond
#
	my $failed;
	while (keys %ids) {
		$result = $q->wait_any(keys %ids);
		$failed = 1,
		last
			unless defined($result) &&
				(ref $result) &&
				(ref $result eq 'HASH');
		map {
			$failed = 1
				unless delete $ids{$_};
		} keys %$result;
		last
			if $failed;
	}
	report_result((!$failed), 'wait_any()');
#
#	test wait_any_until
#
	%ids = ();

	$ids{$q->publish('wait', '3')} = 1;
	map { $ids{$q->publish('foo', 'bar')} = 1; } (2..10);
	$failed = undef;

	$result = $q->wait_any_until(1, keys %ids);
	if ($result) {
		report_result(undef, 'wait_any_until()');
	}
	else {
		while (keys %ids) {
			$result = $q->wait_any_until(5, keys %ids);
			$failed = 1,
			last
				unless defined($result) &&
					(ref $result) &&
					(ref $result eq 'HASH');
			map {
				$failed = 1
					unless delete $ids{$_};
			} keys %$result;
			last
				if $failed;
		}
		report_result((!$failed), 'wait_any_until()');
	}
#
#	test wait_all
#
	%ids = ();
	map { $ids{$q->publish('foo', 'bar')} = 1; } (1..10);
#
#	test available()
#
	sleep 1;
	my @avail = $q->available;
	report_result((scalar @avail), 'available (array)');

	$id = $q->available;
	report_result($id, 'available (scalar)');

	$id = keys %ids;
	@avail = $q->available($id);
	report_result((scalar @avail), 'available (id)');
#
#	make sure all ids respond
#
	$result = $q->wait_all(keys %ids);
	unless (defined($result) &&
		(ref $result) &&
		(ref $result eq 'HASH') &&
		(scalar keys %ids == scalar keys %$result)) {
		report_result(undef, 'wait_all()');
	}
	else {
		map { $failed = 1 unless delete $ids{$_}; } keys %$result;
		report_result((!($failed || scalar %ids)), 'wait_all()');
	}
#
#	test wait_all_until
#
	%ids = ();
	map { $ids{$q->publish('wait', '1')} = 1; } (1..10);
#
#	make sure all ids respond
#
	$result = $q->wait_all_until(1, keys %ids);
	if (defined($result)) {
		report_result(undef, 'wait_all_until()');
	}
	else {
	# may need a warning print here...
		$result = $q->wait_all_until(20, keys %ids);
		map { $failed = 1 unless delete $ids{$_}; } keys %$result;
		report_result((!($failed || scalar keys %ids)), 'wait_all_until()');
	}
#
#	test cancel()/cancel_all():
#	post a waitop
# 	post a no wait
#	wait a bit for server to pick up the first
#	cancel the nowait
#	check the pending count for zero
#	wait for waitop to finish
#
	$id = $q->publish('wait', 5);
	$id1 = $q->publish('foo', 'bar');
	$result = $q->wait_until($id, 3);
	$q->cancel($id1);
#print "Cancel: pending :", $q->pending, "\n";
	report_result((!$q->pending),'cancel()');
	$result = $q->wait($id);
#
#	do same, but add multiple and cancel all
#
	$id = $q->publish('wait', 5);
	$id1 = $q->publish('first', 'bar');
	$id2 = $q->publish('second', 'bar');
	$result = $q->wait_until($id, 1);
	$q->cancel_all();
#print "Cancel all: pending :", $q->pending, " avail ", $q->available, "\n";
	report_result((!($q->pending || $q->available)), 'cancel_all()');

####################################################################
#
#	END PUBLISH TESTS
#
####################################################################
####################################################################
#
#	BEGIN ENQUEUE TESTS
#
####################################################################
#
#	create subset of subs
#
	if ((scalar @subids) > 1) {
		my $subset = (scalar @subids) >> 1;
		@subids = @subids[1..$subset];
	}
#
#	test enqueue_simplex
#
	$id = $q->enqueue_simplex(\@subids, 'foo', 'bar');
	report_result(defined($id), 'enqueue_simplex()');
#
#	test enqueue
#
	$id = $q->enqueue(\@subids, 'foo', 'bar');
	report_result(defined($id), 'enqueue()');
#
#	test ready(); don't care about outcome
#	(prolly need eval here)
#
	$result = $q->ready($id);
	report_result(1, 'ready()');
#
#	test wait()
#
	$result = $q->wait($id);
	report_result(
		(defined($result) && has_all_subs($result, \@subids, 'foo', 'bar')), 'wait()');
#
#	test dequeue_response
#
	$id = $q->enqueue(\@subids, 'foo', 'bar');
	$result = $q->dequeue_response($id);
	report_result(
		(defined($result) && has_all_subs($result, \@subids, 'foo', 'bar')), 'dequeue_response()');
#
#	test TQM_FIRST_ONLY
#
	$id = $q->enqueue(-1, 'foo', 'bar');
	$result = $q->dequeue_response($id);
	report_result(
		(defined($result) && has_all_subs($result, -1, 'foo', 'bar')), 'FIRST_ONLY');
#
#	test Queueable enqueue
#
	$id = $q->enqueue(\@subids, 'foo', $qable);
	report_result(defined($id), 'enqueue() Queueable');

	$result = $q->wait($id);
	report_result(
		(defined($result) && has_all_subs($result, \@subids, 'foo', $qable)), 'wait() Queueable()');
#
#	test wait_until, enqueue_urgent
#
	$id = $q->enqueue(\@subids, 'wait', 3);
	$id1 = $q->enqueue(\@subids, 'foo', 'bar');
	$result = $q->wait_until($id, 1);
	report_result((!defined($result)), 'wait_until() expires');

	$id2 = $q->enqueue_urgent(\@subids, 'urgent', 'entry');
#
#	should get wait reply here
#
	$result = $q->wait_until($id, 5);
	report_result(
		(defined($result) && has_all_subs($result, \@subids, 'wait', 3)), 'wait_until()');
#
#	should get urgent reply here
#
	$result = $q->wait($id2);
	report_result(
		(defined($result) && has_all_subs($result, \@subids, 'urgent', 'entry')), 'enqueue_urgent()');
#
#	should get normal reply here
#
	$result = $q->wait($id1);
	report_result(
		(defined($result) && has_all_subs($result, \@subids, 'foo', 'bar')), 'enqueue()');
#
#	test wait_any: need to queue up several
#
	%ids = ();

	map { $ids{$q->enqueue(\@subids, 'foo', 'bar')} = 1; } (1..10);
#
#	repeat here until all ids respond
#
	$failed = undef;
	while (keys %ids) {
		$result = $q->wait_any(keys %ids);
		$failed = 1,
		last
			unless defined($result) &&
				(ref $result) &&
				(ref $result eq 'HASH');
		map {
			$failed = 1
				unless delete $ids{$_};
		} keys %$result;
		last
			if $failed;
	}
	report_result((!$failed), 'wait_any()');
#
#	test wait_any_until
#
	%ids = ();

	$ids{$q->enqueue(\@subids, 'wait', '3')} = 1;
	map { $ids{$q->enqueue(\@subids, 'foo', 'bar')} = 1; } (2..10);
	$failed = undef;

	$result = $q->wait_any_until(1, keys %ids);
	if ($result) {
		report_result(undef, 'wait_any_until()');
	}
	else {
		while (keys %ids) {
			$result = $q->wait_any_until(5, keys %ids);
			$failed = 1,
			last
				unless defined($result) &&
					(ref $result) &&
					(ref $result eq 'HASH');
			map {
				$failed = 1
					unless delete $ids{$_};
			} keys %$result;
			last
				if $failed;
		}
		report_result((!$failed), 'wait_any_until()');
	}
#
#	test wait_all
#
	%ids = ();
	map { $ids{$q->enqueue(\@subids, 'foo', 'bar')} = 1; } (1..10);
#
#	test available()
#
	sleep 1;
	@avail = $q->available;
	report_result((scalar @avail), 'available (array)');

	$id = $q->available;
	report_result($id, 'available (scalar)');

	$id = keys %ids;
	@avail = $q->available($id);
	report_result((scalar @avail), 'available (id)');
#
#	make sure all ids respond
#
	$result = $q->wait_all(keys %ids);
	unless (defined($result) &&
		(ref $result) &&
		(ref $result eq 'HASH') &&
		(scalar keys %ids == scalar keys %$result)) {
		report_result(undef, 'wait_all()');
	}
	else {
		map { $failed = 1 unless delete $ids{$_}; } keys %$result;
		report_result((!($failed || scalar %ids)), 'wait_all()');
	}
#
#	test wait_all_until
#
	%ids = ();
	map { $ids{$q->enqueue(\@subids, 'wait', '1')} = 1; } (1..10);
#
#	make sure all ids respond
#
	$result = $q->wait_all_until(1, keys %ids);
	if (defined($result)) {
		report_result(undef, 'wait_all_until()');
	}
	else {
	# may need a warning print here...
		$result = $q->wait_all_until(20, keys %ids);
		map { $failed = 1 unless delete $ids{$_}; } keys %$result;
		report_result((!$failed || scalar keys %ids), 'wait_all_until()');
	}
#
#	test cancel()/cancel_all():
#	post a waitop
# 	post a no wait
#	wait a bit for server to pick up the first
#	cancel the nowait
#	check the pending count for zero
#	wait for waitop to finish
#
	$id = $q->enqueue(\@subids, 'wait', 5);
	$id1 = $q->enqueue(\@subids, 'foo', 'bar');
	$result = $q->wait_until($id, 3);
	$q->cancel($id1);
#print "Cancel: pending :", $q->pending, "\n";
	report_result((!$q->pending), 'cancel()');
	$result = $q->wait($id);
#
#	do same, but add multiple and cancel all
#
	$id = $q->enqueue(\@subids, 'wait', 5);
	$id1 = $q->enqueue(\@subids, 'first', 'bar');
	$id2 = $q->enqueue(\@subids, 'second', 'bar');
	$result = $q->wait_until($id, 1);
	$q->cancel_all();
#print "Cancel all: pending :", $q->pending, " avail ", $q->available, "\n";
	report_result((!($q->pending || $q->available)), 'cancel_all()');

####################################################################
#
#	END ENQUEUE TESTS
#
####################################################################

#
#	kill the thread; also tests urgent i/f
#
	$id = $q->publish_urgent('stop');
	report_result($id, 'publish_urgent()');
	$result = $q->wait($id);
	$_->join
		foreach (@subs);
#
#	test publish wo/ a subscriber
#
	report_result((!$q->publish('no subscriber')), 'publish() wo/ subscriber');

}	#end foreach server method

$testno--;
$loaded = 1;

