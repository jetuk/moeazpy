"""
Tests for moeazpy.Population

"""
import pytest

from moeazpy.clients.population import InsufficientPopulationEntries, UniqueIDAlreadyExists, Population


@pytest.fixture()
def pop():
    return Population(capacity=100)


@pytest.fixture()
def pop_with_children(pop):
    for i in range(10):
        pop.insert_child(list(range(10)))
    return pop


def test_pop_init(pop):
    assert pop.capacity == 100


def test_pop_fetch_random_adult(pop):
    """ Test retrieving a random adult of the population """

    with pytest.raises(InsufficientPopulationEntries):
        pop.fetch_random_adults()


def test_pop_fetch_random_child(pop):
    """ Test retrieving a random child of the population """

    with pytest.raises(InsufficientPopulationEntries):
        pop.fetch_random_children()


def test_pop_insert_child(pop):
    """ Test inserting a child into the population """

    assert len(pop.children) == 0

    variables = list(range(10))
    child = pop.insert_child(variables)

    assert len(pop.children) == 1
    assert child.uid in pop.children

    # Now test adding two children with the same UID
    with pytest.raises(UniqueIDAlreadyExists):
        pop.insert_child(list(range(10)), uid=child.uid)


def test_pop_grow_child(pop_with_children):
    """ Test growing a child  """

    pop = pop_with_children

    pop.capacity = len(pop.children)

    child = pop.fetch_random_children()[0]

    # There is space in the adult population so this should be added directly adults
    pop.grow_child(child.uid, "variables", "objectives")
    assert child.uid not in pop.children
    assert child.uid not in pop.adolescents
    assert child.uid in pop.adults

    # Limit the population
    pop.capacity = 1

    child = pop.fetch_random_children()[0]
    # There is no space in the adult population so this should be added directly adolescents
    pop.grow_child(child.uid, "variables", "objectives")
    assert child.uid not in pop.children
    assert child.uid in pop.adolescents
    assert child.uid not in pop.adults

    with pytest.raises(KeyError):
        pop.grow_child("a uid that doesn't exist", 'variables', 'objectives')


def test_pop_grow_adolescent(pop_with_children):
    """ Test growing an adolescent """

    pop = pop_with_children
    pop.capacity = len(pop.children) // 2

    for uid in list(pop.children.keys()):
        pop.grow_child(uid, 'variables', 'objectives')

    assert len(pop.children) == 0
    assert len(pop.adolescents) == pop.capacity
    assert len(pop.adults) == pop.capacity

    adolescent = pop.fetch_random_adolescents()[0]
    parents = [p.uid for p in pop.fetch_random_adults(n=2)]

    pop.dominate_adults(adolescent.uid, parents)

    assert adolescent.uid not in pop.adolescents
    assert adolescent.uid in pop.adults
    for uid in parents:
        assert uid not in pop.adults


