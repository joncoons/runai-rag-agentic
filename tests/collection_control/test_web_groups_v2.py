"""Offline contracts for text-first discovery reconciliation."""
import copy
import unittest
from collection_control.repository import Conflict
from collection_control.web_groups import merge_graph


def page(uri='https://example.test/page',revision='page:1',relations=None):
    return {'document_id':'doc:'+revision,'source_revision_id':revision,'canonical_uri':uri,'relations':relations or []}


def relation(uri='https://example.test/asset',kind='binary',request_id='request:1'):
    return {'request_id':request_id,'canonical_uri':uri,'kind':kind,'locator':'a:1','alt':''}


def manifest(pages,policy=None):
    return {'schema_version':'web-page-group.v2','coverage':'complete','policy':policy or {'include_images':True},'pages':pages}


class DiscoveryGraphTests(unittest.TestCase):
    def test_pending_discovery_publishes_without_raw_bytes(self):
        graph=merge_graph({'schema_version':'web-published-graph.v1','pages':[]},manifest([page(relations=[relation()])]))
        self.assertEqual(graph['schema_version'],'web-published-graph.v2')
        self.assertEqual(graph['pages'][0]['assets'],[])
        self.assertEqual(graph['pages'][0]['relations'],[relation()])

    def test_pending_refresh_preserves_selected_revision_for_existing_link(self):
        old_page=page(relations=[relation()]);old_page['policy']={'include_images':True}
        asset={'kind':'binary','canonical_uri':relation()['canonical_uri'],'source_revision_id':'asset:1','document_id':'asset-doc'}
        old_page['assets']=[asset]
        graph=merge_graph({'schema_version':'web-published-graph.v2','pages':[old_page]},manifest([page(revision='page:2',relations=[relation(request_id='request:2')])]))
        self.assertEqual(graph['pages'][0]['assets'],[asset])

    def test_complete_observation_unlinks_only_observed_page(self):
        a=page(relations=[relation()]);a['assets']=[{'kind':'binary','canonical_uri':relation()['canonical_uri'],'source_revision_id':'asset:1'}]
        b=copy.deepcopy(a);b['canonical_uri']='https://example.test/other';b['document_id']='other-doc'
        graph=merge_graph({'schema_version':'web-published-graph.v2','pages':[a,b]},manifest([page(revision='page:2')]))
        self.assertEqual(graph['pages'][0]['assets'],b['assets'])
        self.assertEqual(graph['pages'][1]['assets'],[])

    def test_policy_change_cannot_authorize_unlink(self):
        a=page(relations=[relation()]);a.update(assets=[],policy={'include_images':True})
        with self.assertRaises(Conflict):
            merge_graph({'schema_version':'web-published-graph.v2','pages':[a]},manifest([page(revision='page:2')],{'include_images':False}))

    def test_duplicate_request_identity_rejected(self):
        with self.assertRaises(Conflict):
            merge_graph({'schema_version':'web-published-graph.v2','pages':[]},manifest([page(relations=[relation(),relation(uri='https://example.test/other')])]))

    def test_new_shared_binary_reference_preserves_known_selected_revision(self):
        old=page(relations=[relation()]);old['assets']=[{'kind':'binary','canonical_uri':relation()['canonical_uri'],'source_revision_id':'asset:1','document_id':'asset-doc'}]
        second=page(uri='https://example.test/second',revision='page:2',relations=[relation(request_id='request:2')])
        graph=merge_graph({'schema_version':'web-published-graph.v2','pages':[old]},manifest([second]))
        graph=merge_graph(graph,manifest([page(revision='page:3')]))
        self.assertEqual(graph['pages'][1]['assets'],old['assets'])

    def test_v1_stays_strict(self):
        with self.assertRaises(Conflict):
            merge_graph({'schema_version':'web-published-graph.v1','pages':[]},{'schema_version':'web-asset-group.v1','coverage':'partial','pages':[]})

class ChildGraphTests(unittest.TestCase):
    def setUp(self):
        self.page=page(relations=[relation(kind='image')]);self.page.update(group_id='group:1',assets=[])
        self.graph={'schema_version':'web-published-graph.v2','pages':[self.page]}
        self.proof={'schema_version':'web-asset-publication.v2','collection_id':'collection','request_id':'request:1','parent_pages':[{k:self.page[k] for k in ('group_id','document_id','source_revision_id','canonical_uri')}], 'asset':{'canonical_uri':relation()['canonical_uri'],'kind':'image','source_revision_id':'image:1','document_id':'image:1','raw_sha256':'a'*64}}

    def apply(self):
        from collection_control.web_children import attach_asset
        return attach_asset(self.graph,self.proof,{'uri':'file:///association','sha256':'b'*64})

    def test_child_attaches_only_exact_discovered_parent(self):
        selected=self.apply()['pages'][0]['assets'][0]
        self.assertEqual(selected['source_revision_id'],'image:1')
        self.assertEqual(self.graph['pages'][0]['assets'],[])

    def test_replacement_parent_fences_late_child(self):
        self.graph['pages'][0]['source_revision_id']='page:2'
        with self.assertRaises(Conflict):self.apply()

    def test_unlinked_parent_fences_late_child(self):
        self.graph['pages'][0]['relations']=[]
        with self.assertRaises(Conflict):self.apply()

    def test_same_uri_different_kind_cannot_attach(self):
        self.proof['asset']['kind']='binary'
        with self.assertRaises(Conflict):self.apply()

    def test_shared_binary_conflicting_revision_requires_coherent_change(self):
        self.proof['asset']['kind']='binary';self.page['relations'][0]['kind']='binary'
        other=copy.deepcopy(self.page);other['canonical_uri']='https://example.test/other';other['assets']=[{**self.proof['asset'],'source_revision_id':'old-revision'}]
        self.graph['pages'].append(other)
        with self.assertRaises(Conflict):self.apply()

class EnrichmentTests(unittest.TestCase):
    def setUp(self):
        import hashlib
        self.contract={'dimensions':2048,'dtype':'float32','normalization':'unit','metric':'IP','schema_version':1,'embedding_checkpoint_sha256':'a'*64}
        self.parent={'record_id':'page-record','text':'HTML text','vector':[1.0]+[0.0]*2047,'metadata':{'origin':'grounded','source_id':'source','source_revision_id':'page:1','content_sha256':hashlib.sha256(b'HTML text').hexdigest(),'acl':['local-operator']}}
        self.images=[{'document_id':'image:1','source_revision_id':'image:1','canonical_uri':'https://example.test/image','stage_key':'stage:1','stage_artifact_sha256':'b'*64,'text':'Native image text'}]

    def test_native_evidence_keeps_parent_identity_and_one_vector(self):
        from collection_control.web_children import enriched_page
        result=enriched_page(self.parent,self.images,[],self.parent['vector'],self.contract,'c'*64,'parent-stage')
        self.assertEqual(result['record_id'],self.parent['record_id'])
        self.assertIn('Native image text',result['text'])
        self.assertEqual(result['metadata']['origin'],'derived')
        self.assertFalse(result['metadata']['generated_annotation'])

    def test_enrichment_cannot_forge_parent_identity(self):
        from collection_control.web_children import enriched_page
        from collection_control.artifacts import admit
        result=enriched_page(self.parent,self.images,[],self.parent['vector'],self.contract,'c'*64,'parent-stage')
        result['metadata']['web_enrichment_derivation']['parent_record_id']='different-parent'
        with self.assertRaises(Conflict):admit({'contract':self.contract,'records':[result]})

class SharedAssociationReplayTests(ChildGraphTests):
    def test_stale_shared_parent_does_not_discard_current_parent(self):
        self.proof['parent_pages'].append({'group_id':'stale-group','document_id':'stale-doc','source_revision_id':'stale-revision','canonical_uri':'https://example.test/stale'})
        graph=self.apply()
        self.assertEqual(graph['pages'][0]['assets'][0]['document_id'],'image:1')

    def test_alternate_association_reference_does_not_republish_same_asset(self):
        from collection_control.web_children import attach_asset
        graph=self.apply()
        replay=attach_asset(graph,self.proof,{'uri':'file:///new-association','sha256':'c'*64})
        self.assertEqual(replay,graph)

class CaptionLifecycleTests(ChildGraphTests):
    def test_caption_waits_before_native_child_publication(self):
        from collection_control.web_children import caption_needs_native_publication
        self.assertTrue(caption_needs_native_publication(self.graph,self.proof,{'group:1'}))

    def test_caption_is_terminal_after_exact_parent_was_replaced(self):
        from collection_control.web_children import caption_needs_native_publication
        self.graph['pages'][0]['source_revision_id']='new-parent'
        self.assertFalse(caption_needs_native_publication(self.graph,self.proof,{'group:1'}))

    def test_caption_is_terminal_after_relationship_unlinked(self):
        from collection_control.web_children import caption_needs_native_publication
        self.graph['pages'][0]['relations']=[]
        self.assertFalse(caption_needs_native_publication(self.graph,self.proof,{'group:1'}))

    def test_caption_still_waits_if_original_parent_not_published(self):
        from collection_control.web_children import caption_needs_native_publication
        self.assertTrue(caption_needs_native_publication({'pages':[]},self.proof,set()))

    def test_native_replacement_drops_only_replaced_images_captions(self):
        from collection_control.web_children import retained_caption_keys
        dependencies=[{'stage_key':'old-caption','document_id':'old-image'},{'stage_key':'other-caption','document_id':'other-image'}]
        self.assertEqual(retained_caption_keys(dependencies,{'new-image':{},'other-image':{}},None),['other-caption'])
