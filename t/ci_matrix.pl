use strict;
use warnings;
use JSON::PP qw(encode_json);

# Pin one representative per supported API boundary, not a version cross product.
my $full = ($ENV{CI_FULL} || '') eq 'true';
my $run = 1;
my $base = $ENV{CI_BASE} || '';
if (!$full && $base =~ /^[0-9a-f]{40}$/ && $base !~ /^0+$/) {
    if (open my $git, '-|', 'git', 'diff', '--name-only', $base, 'HEAD') {
        my @changed = <$git>;
        if (close $git && @changed) {
            chomp @changed;
            $run = scalar grep {
                !m{^(?:README(?:\.[A-Za-z-]+)?\.md|docs/.*|LICENSE|COPYING|\.github/(?:ISSUE_TEMPLATE|PULL_REQUEST_TEMPLATE)/.*)$}
            } @changed;
        }
    }
}

my @integration;
for my $core ('nginx', 'nginx-ext') {
    push @integration,
        {name => "$core-static", core => $core, nginx => '1.31.8', dynamic => 'false', sanitize => 'false', cc => 'gcc', cxx => 'g++'},
        {name => "$core-dynamic", core => $core, nginx => '1.31.8', dynamic => 'true', sanitize => 'false', cc => 'gcc', cxx => 'g++'},
        {name => "$core-asan", core => $core, nginx => '1.31.8', dynamic => 'false', sanitize => 'true', cc => 'gcc', cxx => 'g++'};
    if ($full) {
        push @integration,
            {name => "$core-clang", core => $core, nginx => '1.31.8', dynamic => 'true', sanitize => 'false', cc => 'clang', cxx => 'clang++'};
    }
}
push @integration,
    {name => 'stable-dynamic', core => 'nginx', nginx => '1.26.3', dynamic => 'true', sanitize => 'false', cc => 'gcc', cxx => 'g++'};
open my $out, '>>', $ENV{GITHUB_OUTPUT} or die "GITHUB_OUTPUT: $!";
print {$out} 'run=', ($run ? 'true' : 'false'), "\n",
             'full=', ($full ? 'true' : 'false'), "\n",
             'integration=', encode_json({include => \@integration}), "\n";
close $out or die $!;
open my $summary, '>>', $ENV{GITHUB_STEP_SUMMARY} or die "GITHUB_STEP_SUMMARY: $!";
print {$summary} "### CI scope\n\n",
    $run ? ($full ? "Extended checks.\n\n" : "Commit checks.\n\n")
         : "Documentation-only change: compilation and runtime jobs skipped.\n\n";
print {$summary} "| Group | NGINX versions |\n|---|---|\n",
    '| Persistent integration | ', join(', ', map { $_->{name} . ' ' . $_->{nginx} } @integration), " |\n";
close $summary or die $!;
