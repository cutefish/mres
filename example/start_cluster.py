from mres.run.sched import Experiment, Triggers
from mres.run.system import MRv1Runtime, HdfsFileSystem
from mres.run.bench import WordCount

def setup_exp(exp):
    exp.settmpdir('/home/xyu40/tmp')
    exp.setconf('topology.script.file.name', 'conf/topology_script.sh')


def setup_phase(fs, rt):
    fs.setnn('192.168.0.100', 9100)
    fs.setdns(['192.168.0.1{:02d}'.format(i) for i in range(1, 25)])
    rt.setjt('192.168.0.100', 9101)
    rt.settts(['192.168.0.1{:02d}'.format(i) for i in range(1, 25)])
    fs.setconf('dfs.name.dir', '${hadoop.tmp.dir}/dfs/name')
    fs.setconf('dfs.data.dir', '${hadoop.tmp.dir}/dfs/data')
    rt.setconf('mapred.tasktracker.map.tasks.maximum', 4)
    rt.setconf('mapred.tasktracker.reduce.tasks.maximum', 2)


exp = Experiment()
exp.settmpdir('')
setup_exp(exp)
exp.set_initsteps([
    exp.updatejars,
    exp.updateconf,
])
for i in range(2):
    phase = exp.newphase()
    rt = phase.newrt(MRv1Runtime)
    fs = phase.newfs(HdfsFileSystem)
    setup_phase(fs, rt)
    phase.set_initsteps([
        rt.updateconf,
        fs.updateconf,
    ])
    last = None
    jobs = []
    for j in range(2):
        job = phase.newjob(WordCount, '/in/{0}, /out/{0}'.format(j))
        job.startif(Triggers.ActionDone(last))
        jobs.append(job)
        last = job
    phase.newaction(fs.CopyToLocal, 'hdfs://logs /home/xyu40/logs').\
        startif(Triggers.ActionDone(last))
    phase.kill().startif(Triggers.ActionFail(jobs))
exp.start()
