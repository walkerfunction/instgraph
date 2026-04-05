package cypher

// compareAny compares two values for ordering in the Cypher context.
func compareAny(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	af, aok := toFloat(a)
	bf, bok := toFloat(b)
	if aok && bok {
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}

	as, aok := a.(string)
	bs, bok := b.(string)
	if aok && bok {
		if as < bs {
			return -1
		}
		if as > bs {
			return 1
		}
		return 0
	}

	return 0
}
