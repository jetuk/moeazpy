

*******************************
Messaging Interface for moeazpy
*******************************


Introduction
============

All messages are encoded JSON dictionaries with the following keys

:**type**: message type (see specific message types defined below)
:**content**: the content of the message
:**compression**: (optional) the type of compression used for the content of the message.




Population publication messages
===============================

:class:`Population`


* status


Population requests
===================

The following is a list of valid requests that can be sent a :class:`PopulationServer`.


Fetching population members
-----------------------------


To fetch a single random member of the population.

:type: fetch_random_member
:content: None


To fetch multiple random members of the population.

:type: fetch_random_members
:content: dictionary with the following keys
    :number_of_members: positive integer




Retrieving population children
------------------------------

To fetch a single random child of the population.

:type: fetch_random_child
:content: None
