#include "msg/Message.h"
#include "msg/DispatchQueue.h"
#include "msg/Messenger.h"
#include "common/ceph_context.h"

#define dout_subsys ceph_subsys_ms
#include "common/debug.h"

#include "adsl/tags.h"
#ifdef ADSLTAG_QUEUEING_OBSERVER
#include "mds/mdstypes.h"
#endif

/*******************
 * DispatchQueue
 */

#undef dout_prefix
#define dout_prefix *_dout << "-- " << msgr->get_myaddr() << " "

#ifdef ADSLTAG_QUEUEING_OBSERVER
// Declared in msg/DispatchQueue.h
std::string DispatchQueue::adsl_get_queueing_observer_string(Message * m)
{
	assert(lock.is_locked_by_me());
	std::stringstream ss;
	ss << "Message type: " << m->get_type_name();
	return ss.str();
	return "OK";
}

int DispatchQueue::check_migration(QueueItem & item, void * arg)
{
	QueueingCounter * qc = (QueueingCounter *)arg;
	if (item.is_code()) {
		qc->conns++; // Connection
	}
	else {
		Message * m = item.get_message();
		if (!m) {
			qc->ignored++;
		}
		else {
			if ((m->get_type() & 0xff00) == MDS_PORT_MIGRATOR) {
				qc->migs++;
			}
			else {
				qc->other++;
			}
#ifdef ADSLTAG_QUEUEING_OBSERVER_SHOW_BLM
			qc->mnames << m->get_type_name() << ' ';
#endif
		}
	}
	return 0;
}

void DispatchQueue::clientreqs_observe_queueing(MClientRequest * m)
{
	if (!m)	return;
	assert(lock.is_locked_by_me());

	QueueingCounter qc;

	// Count
	int checked = mqueue.traverse(check_migration, &qc);
	int total = mqueue.length();
	ldout(cct, 0) << ADSLTAG_QUEUEING_OBSERVER << "Checked " << checked << " out of " << total << dendl;

	m->migs_in_queue = qc.migs;
	m->other_in_queue = qc.other + qc.conns;
#ifdef ADSLTAG_QUEUEING_OBSERVER_SHOW_BLM
	m->bm_names = qc.mnames.str();
#endif
}
#endif
