

# fungsi menghapus elemen list dalam hirarki
def traverse_nested_list(my_nested_list, itemToRemove):
    results=[]
    for my_item in my_nested_list.copy():
        if not isinstance(my_item, list):
            # Insert condition
            if my_item == itemToRemove:
                pass
            else:
                results.append(my_item)
        if isinstance(my_item, list):
            results.append(traverse_nested_list(my_item, itemToRemove))
    return results