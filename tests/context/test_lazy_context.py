import pytest

from pypipes.context import LazyContextCollection, IContextFactory, INamedContextFactory


class SumContextFactory(IContextFactory):
    def __init__(self, *keys):
        self.keys = set(keys)

    def __call__(self, context_dict):
        return sum(context_dict[context_name]
                   for context_name in context_dict
                   if context_name in self.keys)


class NamedSumContextFactory(SumContextFactory, INamedContextFactory):
    def __call__(self, context_dict, context_name=None):
        result = super(NamedSumContextFactory, self).__call__(context_dict)
        return '{} = {}'.format(context_name, result)


@pytest.mark.parametrize('key, expected', [
    ('a', 2),
    ('sum_a_b', 5),
    ('sum_a_b_c', 9),
    ('named_value', 'named_value = 9')
])
def test_lazy_context_collection(key, expected):
    context = LazyContextCollection(
        a=2,
        b=3,
        c=4,
        sum_a_b=SumContextFactory('a', 'b'),  # lazy context
        sum_a_b_c=SumContextFactory('sum_a_b', 'c'),
        named_value=NamedSumContextFactory('sum_a_b_c')
    )

    assert context[key] == expected
    assert context.get(key) == expected


def test_lazy_context_collection_unknown():
    context = LazyContextCollection()  # empty collection
    assert context.get('unknown') is None
    assert context.get('unknown', 100) == 100


def test_lazy_context_collection_cycle():
    # we have cycle in lazy context calculation  lazy1-> lazy2 -> lazy3 -> lazy1
    context = LazyContextCollection(
        lazy1=SumContextFactory('lazy2'),
        lazy2=SumContextFactory('lazy3'),
        lazy3=SumContextFactory('lazy1')
    )

    with pytest.raises(KeyError):
        assert not context['lazy1']

    with pytest.raises(KeyError):
        assert not context.get('lazy2')
