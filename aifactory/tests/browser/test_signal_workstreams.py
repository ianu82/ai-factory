"""The real Signals page lets a person resolve held and stale assignments."""
import pytest


@pytest.mark.parametrize('stale', [False, True])
def test_signal_assignment_selector(signed_in_page, scenario, navigate, stale):
    from playwright.sync_api import expect

    page = signed_in_page
    root = f"/api/workspaces/{scenario['workspace_id']}"
    api = page.context.request
    stream = api.post(root + '/workstreams', data={'name': 'Platform', 'goal': 'Delivery'}).json()
    data = {'proposed_workstream_id': stream['id']} if stale else {
        'workstream_assignment_reason': 'Conflicting labels. Choose an active workstream or No workstream.'}
    # Keep the real backend for acceptance; mock only the provider proposal read.
    signal = next(s for s in api.get(root + '/signals').json() if s['id'] == scenario['feedback_id'])
    signal['data'] = data
    if stale:
        assert api.patch(root + '/workstreams/' + stream['id'], data={'state': 'archived'}).status == 200
    page.route('**' + root + '/signals', lambda route: route.fulfill(json=[signal]))
    navigate(page, 'Signals')
    picker = page.get_by_role('combobox', name='Workstream for ' + signal['title'])
    expect(picker).to_be_visible()
    if stale:
        expect(picker.get_by_role('option', name='Platform', exact=True)).to_have_count(0)
    else:
        expect(page.get_by_role('button', name='Make it an outcome')).to_be_disabled()
    picker.select_option('' if stale else stream['id'])
    with page.expect_request('**' + root + '/signals/' + signal['id']) as request:
        page.get_by_role('button', name='Make it an outcome').click()
    assert request.value.post_data_json['workstream_id'] == (None if stale else stream['id'])
    expect(page).to_have_url(__import__('re').compile('/outcomes/[^/]+$'))
    created = api.get(root + '/outcomes/' + page.url.rsplit('/', 1)[1]).json()['outcome']
    assert created['workstream_id'] == (None if stale else stream['id'])
