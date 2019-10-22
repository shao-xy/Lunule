#include "IPCMessagePreDecoder.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_ipc

#undef dout_prefix
#define dout_prefix *_dout << "ipc.MessagePreDecoder "

Message * IPCMessagePreDecoder::decode(CephContext * cct, ceph_msg_header & header, ceph_msg_footer & footer, bufferlist & front, bufferlist & middle, bufferlist & data, Connection * conn)
{
  int type = header.type;
  // make message
  Message *m = generate_typed_message(type);

  m->set_cct(cct);

  // m->header.version, if non-zero, should be populated with the
  // newest version of the encoding the code supports.  If set, check
  // it against compat_version.
  if (m->get_header().version &&
      m->get_header().version < header.compat_version) {
    if (cct) {
      ldout(cct, 0) << "will not decode message of type " << type
		    << " version " << header.version
		    << " because compat_version " << header.compat_version
		    << " > supported version " << m->get_header().version << dendl;
      if (cct->_conf->ms_die_on_bad_msg)
	ceph_abort();
    }
    m->put();
    return 0;
  }

  m->set_connection(conn);
  m->set_header(header);
  m->set_footer(footer);
  m->set_payload(front);
  m->set_middle(middle);
  m->set_data(data);

  try {
    m->decode_payload();
  }
  catch (const buffer::error &e) {
    if (cct) {
      lderr(cct) << "failed to decode message of type " << type
		 << " v" << header.version
		 << ": " << e.what() << dendl;
      ldout(cct, cct->_conf->ms_dump_corrupt_message_level) << "dump: \n";
      m->get_payload().hexdump(*_dout);
      *_dout << dendl;
      if (cct->_conf->ms_die_on_bad_msg)
	ceph_abort();
    }
    m->put();
    return 0;
  }

  // done!
  return m;
}
